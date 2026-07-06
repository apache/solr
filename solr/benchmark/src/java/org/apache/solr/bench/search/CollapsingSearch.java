/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.bench.search;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1)
@Warmup(time = 5, iterations = 5)
@Measurement(time = 15, iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(value = 1)
public class CollapsingSearch {

  static final String COLLECTION = "collapse_bench";

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"2000000"})
    int numDocs;

    @Param({"100000"})
    int numGroups;

    @Param({"1", "10"})
    int numSegments;

    final QueryRequest qSimple =
        new QueryRequest(new SolrQuery("q", "*:*", "rows", "10", "cache", "false"));

    final QueryRequest qCollapseWithoutSort =
        new QueryRequest(
            new SolrQuery(
                "q", "*:*",
                "fq", "{!collapse field=group_s_dv cache=false}",
                "rows", "10",
                "cache", "false"));

    final QueryRequest qCollapseByStr =
        new QueryRequest(
            new SolrQuery(
                "q", "*:*",
                "fq", "{!collapse field=group_s_dv sort='str_s_dv asc' cache=false}",
                "rows", "10",
                "cache", "false"));

    final QueryRequest qCollapseByDate =
        new QueryRequest(
            new SolrQuery(
                "q", "*:*",
                "fq", "{!collapse field=group_s_dv sort='date_dt_dv asc' cache=false}",
                "rows", "10",
                "cache", "false"));

    final QueryRequest qCollapseByLong =
        new QueryRequest(
            new SolrQuery(
                "q", "*:*",
                "fq", "{!collapse field=group_s_dv sort='long_l_dv asc' cache=false}",
                "rows", "10",
                "cache", "false"));

    final QueryRequest qCollapseByDateAndStr =
        new QueryRequest(
            new SolrQuery(
                "q", "*:*",
                "fq",
                    "{!collapse field=group_s_dv sort='date_dt_dv asc, str_s_dv asc' cache=false}",
                "rows", "10",
                "cache", "false"));

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      System.setProperty("commitwithin.softcommit", "false");
      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, 1, 1);

      indexDocs(miniClusterState);
      miniClusterState.forceMerge(COLLECTION, numSegments);

      int actualSegments = getActualSegmentCount(miniClusterState);
      BaseBenchState.log(
          "CollapsingSearch ready: numDocs="
              + numDocs
              + " numGroups="
              + numGroups
              + " numSegments(requested)="
              + numSegments
              + " numSegments(actual)="
              + actualSegments);
    }

    @Setup(Level.Iteration)
    public void setupIteration(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws SolrServerException, IOException {
      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);
      miniClusterState.client.request(reload);
    }

    @TearDown(Level.Trial)
    public void teardown(MiniClusterState.MiniClusterBenchState miniClusterState) throws Exception {
      CollectionAdminRequest.deleteCollection(COLLECTION).process(miniClusterState.client);
    }

    private int pickGroup(int docId) {
      return docId % numGroups;
    }

    private int getActualSegmentCount(MiniClusterState.MiniClusterBenchState state)
        throws SolrServerException, IOException {
      SolrQuery lukeQuery = new SolrQuery();
      lukeQuery.set(CommonParams.QT, "/admin/luke");
      lukeQuery.set("show", "index");
      var resp = state.client.query(COLLECTION, lukeQuery);
      Object segCount = resp.getResponse().findRecursive("index", "segmentCount");
      return segCount instanceof Number ? ((Number) segCount).intValue() : -1;
    }

    private void indexDocs(MiniClusterState.MiniClusterBenchState state) throws Exception {
      int docsPerSegment = numDocs / numSegments;
      String[] groupIds = new String[numGroups];
      for (int i = 0; i < numGroups; i++) {
        groupIds[i] = String.format(Locale.ROOT, "group_%06d", i);
      }
      Random rng = new Random(42);
      Instant baseDate = Instant.parse("2020-01-01T00:00:00Z");

      int docId = 0;
      for (int seg = 0; seg < numSegments; seg++) {
        int segDocs = (seg < numSegments - 1) ? docsPerSegment : (numDocs - docId);
        UpdateRequest req = new UpdateRequest();
        for (int i = 0; i < segDocs; i++) {
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", String.valueOf(docId));
          doc.addField("group_s_dv", groupIds[pickGroup(docId)]);
          docId++;
          doc.addField("str_s_dv", UUID.randomUUID().toString());
          doc.addField(
              "date_dt_dv",
              DateTimeFormatter.ISO_INSTANT.format(baseDate.plusSeconds(rng.nextInt(10_000_000))));
          doc.addField("long_l_dv", rng.nextInt(10_000_000));
          req.add(doc);
        }
        req.commit(state.client, COLLECTION);
      }
    }
  }

  @Benchmark
  public Object simple(BenchState b, MiniClusterState.MiniClusterBenchState cluster)
      throws SolrServerException, IOException {
    return cluster.client.request(b.qSimple, COLLECTION);
  }

  @Benchmark
  public Object collapseWithoutSort(BenchState b, MiniClusterState.MiniClusterBenchState cluster)
      throws SolrServerException, IOException {
    return cluster.client.request(b.qCollapseWithoutSort, COLLECTION);
  }

  @Benchmark
  public Object collapseByStr(BenchState b, MiniClusterState.MiniClusterBenchState cluster)
      throws SolrServerException, IOException {
    return cluster.client.request(b.qCollapseByStr, COLLECTION);
  }

  @Benchmark
  public Object collapseByDate(BenchState b, MiniClusterState.MiniClusterBenchState cluster)
      throws SolrServerException, IOException {
    return cluster.client.request(b.qCollapseByDate, COLLECTION);
  }

  @Benchmark
  public Object collapseByLong(BenchState b, MiniClusterState.MiniClusterBenchState cluster)
      throws SolrServerException, IOException {
    return cluster.client.request(b.qCollapseByLong, COLLECTION);
  }

  @Benchmark
  public Object collapseByDateAndStr(BenchState b, MiniClusterState.MiniClusterBenchState cluster)
      throws SolrServerException, IOException {
    return cluster.client.request(b.qCollapseByDateAndStr, COLLECTION);
  }
}

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
package org.apache.solr.bench.index;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.longs;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(4)
@Warmup(time = 10, iterations = 3)
@Measurement(time = 20, iterations = 4)
@Fork(value = 1)
@Timeout(time = 60)
// A benchmark to experiment with the performance of distributed indexing.
public class CloudIndexing {

  @State(Scope.Benchmark)
  public static class BenchState {

    static final String COLLECTION = "testCollection";

    @Param({"1"})
    public int scale;

    @Param("4")
    int nodeCount;

    @Param("5")
    int numShards;

    @Param({"1", "3"})
    int numReplicas;

    @Param({"0", "15", "30", "70", "100", "500", "1000"})
    int useStringUtf8Over;

    @Param({"true", "false"})
    boolean directBuffer;

    private final org.apache.solr.bench.Docs largeDocs;
    private Iterator<SolrInputDocument> largeDocIterator;

    private final org.apache.solr.bench.Docs smallDocs;
    private Iterator<SolrInputDocument> smallDocIterator;

    public BenchState() {

      largeDocs =
          docs()
              .field("id", integers().incrementing())
              .field(strings().basicLatinAlphabet().multi(312).ofLengthBetween(30, 64))
              .field(strings().basicLatinAlphabet().multi(312).ofLengthBetween(30, 64))
              .field(integers().all())
              .field(integers().all())
              .field(integers().all())
              .field(longs().all())
              .field(longs().all());

      try {
        largeDocIterator = largeDocs.preGenerate(50000);

        smallDocs =
            docs()
                .field("id", integers().incrementing())
                .field("text", strings().basicLatinAlphabet().multi(2).ofLengthBetween(20, 32))
                .field("int1_i", integers().all())
                .field("int2_i", integers().all())
                .field("long1_l", longs().all());

        smallDocIterator = smallDocs.preGenerate(50000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    public SolrInputDocument getLargeDoc() {
      if (!largeDocIterator.hasNext()) {
        largeDocIterator = largeDocs.generatedDocsIterator();
      }
      return largeDocIterator.next();
    }

    public SolrInputDocument getSmallDoc() {
      if (!smallDocIterator.hasNext()) {
        smallDocIterator = smallDocs.generatedDocsIterator();
      }
      return smallDocIterator.next();
    }

    @Setup(Level.Trial)
    public void doSetup(MiniClusterState.MiniClusterBenchState miniClusterState) throws Exception {
      System.setProperty("useStringUtf8Over", Integer.toString(useStringUtf8Over));
      System.setProperty("httpClientDirectBuffer", Boolean.toString(directBuffer));

      System.setProperty("mergePolicyFactory", "org.apache.solr.index.NoMergePolicyFactory");
      miniClusterState.startMiniCluster(nodeCount);
      miniClusterState.createCollection(COLLECTION, numShards, numReplicas);
    }

    @TearDown(Level.Trial)
    public void doTearDown(
        MiniClusterState.MiniClusterBenchState miniClusterState, BenchmarkParams benchmarkParams)
        throws Exception {

      // miniClusterState.shutdownMiniCluster(benchmarkParams);
    }
  }

  @Benchmark
  @Timeout(time = 300)
  public Object indexLargeDoc(
      MiniClusterState.MiniClusterBenchState miniClusterState, BenchState state) throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(
        miniClusterState.nodes.get(miniClusterState.getRandom().nextInt(state.nodeCount)));
    SolrInputDocument doc = state.getLargeDoc();
    updateRequest.add(doc);

    return miniClusterState.client.request(updateRequest, BenchState.COLLECTION);
  }

  @Benchmark
  @Timeout(time = 300)
  public Object indexSmallDoc(
      MiniClusterState.MiniClusterBenchState miniClusterState, BenchState state) throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(
        miniClusterState.nodes.get(miniClusterState.getRandom().nextInt(state.nodeCount)));
    SolrInputDocument doc = state.getSmallDoc();
    updateRequest.add(doc);

    return miniClusterState.client.request(updateRequest, BenchState.COLLECTION);
  }
}

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.bench.generators.IntegersDSL;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.SolrQueryResponse;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.dates;
import static org.apache.solr.bench.generators.SourceDSL.doubles;
import static org.apache.solr.bench.generators.SourceDSL.floats;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.longs;
import static org.apache.solr.bench.generators.SourceDSL.strings;

@Fork(value = 1)
@Warmup(time = 5, iterations = 3)
@Measurement(time = 5, iterations = 5)
@Threads(value = 1)
public class NumericSearch {

  static final String COLLECTION = "c1";

  @State(Scope.Benchmark)
  public static class BenchState {

    int setQuerySize = 100; // TODO: Params
    String basePath;

    SolrGen<Integer> setValues;

    List<String> queries = new ArrayList<>();

    int count;

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      miniClusterState.setUseHttp1(true);
      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, 1, 1);
      int maxCardinality = 100000;
      setValues = integers().allWithMaxCardinality(maxCardinality);
      Docs docs =
              docs()
                      .field("id", integers().incrementing())
                      .field("low_cardinality_i_dv", setValues)
                      .field("low_cardinality_i", setValues)
                      .field("low_cardinality_d", setValues)
                      .field("low_cardinality_d_dv", setValues)
                      .field("low_cardinality_l", setValues)
                      .field("low_cardinality_l_dv", setValues)
                      .field("low_cardinality_f", setValues)
                      .field("low_cardinality_f_dv", setValues);
                      //.field("low_cardinality_dt", setValues);

      miniClusterState.index(COLLECTION, docs, 30 * 1000);
      basePath = miniClusterState.nodes.get(0);
      SolrQuery q = new SolrQuery("*:*");
      q.setParam("facet", "true");
      q.setParam("facet.field", "low_cardinality_i_dv");
      q.setParam("facet.limit", String.valueOf(maxCardinality));
      QueryRequest req = new QueryRequest(q);
      req.setBasePath(basePath);
      Set<String> values = req.process(miniClusterState.client, COLLECTION).getFacetField("low_cardinality_i_dv").getValues().stream().map(FacetField.Count::getName).collect(Collectors.toSet());
      Iterator<String> valueIterator = values.iterator();
      StringBuilder builder = new StringBuilder();
      while (valueIterator.hasNext()) {
//        builder.append("(");
        for (int i = 0; i < setQuerySize ; i++) {
          if (!valueIterator.hasNext()) {
            break;
          }
          builder.append(valueIterator.next()).append(",");
        }
//        builder.append(")");
        builder.setLength(builder.length() - 1);
        queries.add(builder.toString());
        builder.setLength(0);
      }
    }

    @Setup(Level.Iteration)
    public void setupIteration(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws SolrServerException, IOException {
      // Reload the collection/core to drop existing caches
      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);
      reload.setBasePath(miniClusterState.nodes.get(0));
      miniClusterState.client.request(reload);
    }

    public QueryRequest intSetQuery(boolean dvs) {
      return setQuery("low_cardinality_i" + (dvs ? "_dv" : ""));
    }

    public QueryRequest longSetQuery(boolean dvs) {
      return setQuery("low_cardinality_l" + (dvs ? "_dv" : ""));
    }

    public QueryRequest doubleSetQuery(boolean dvs) {
      return setQuery("low_cardinality_d" + (dvs ? "_dv" : ""));
    }

    public QueryRequest floatSetQuery(boolean dvs) {
      return setQuery("low_cardinality_f" + (dvs ? "_dv" : ""));
    }

    QueryRequest setQuery(String field) {
      QueryRequest q = new QueryRequest(new SolrQuery("q", "({!terms f='" + field + "'}" + queries.get(count++ % queries.size()) + ")"));
      q.setBasePath(basePath);
      return q;
    }
  }

  @Benchmark
  public Object intSet(
          Blackhole blackhole, BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response = benchState.intSetQuery(false).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object longSet(
          Blackhole blackhole, BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
          throws SolrServerException, IOException {
    QueryResponse response = benchState.longSetQuery(false).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object floatSet(
          Blackhole blackhole, BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
          throws SolrServerException, IOException {
    QueryResponse response = benchState.floatSetQuery(false).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object doubleSet(
          Blackhole blackhole, BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
          throws SolrServerException, IOException {
    QueryResponse response = benchState.doubleSetQuery(false).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object intDvSet(
          Blackhole blackhole, BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
          throws SolrServerException, IOException {
    QueryResponse response = benchState.intSetQuery(true).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object longDvSet(
          Blackhole blackhole, BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
          throws SolrServerException, IOException {
    QueryResponse response = benchState.longSetQuery(true).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object floatDvSet(
          Blackhole blackhole, BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
          throws SolrServerException, IOException {
    QueryResponse response = benchState.floatSetQuery(true).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object doubleDvSet(
          Blackhole blackhole, BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
          throws SolrServerException, IOException {
    QueryResponse response = benchState.doubleSetQuery(true).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }
}

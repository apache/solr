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

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
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

@Fork(value = 1)
@Warmup(time = 5, iterations = 5)
@Measurement(time = 5, iterations = 5)
@Threads(value = 1)
public class NumericSearch {

  static final String COLLECTION = "c1";

  @State(Scope.Benchmark)
  public static class BenchState {

    int setQuerySize = 20; // TODO: Params
    String termQueryField = "term_low_s";
    String basePath;
    SolrGen<Integer> setValues;
    SolrGen<String> lowCardinalityTerms;
    SolrGen<String> highCardinalityTerms;
    Iterator<String> lowCardTerms;
    Iterator<String> highCardTerms;
    Iterator<String> queries;

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      miniClusterState.setUseHttp1(true);
      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, 1, 1);
      int maxCardinality = 10000;
      int numDocs = 2000000;
      setValues = integers().allWithMaxCardinality(maxCardinality);
      lowCardinalityTerms = strings().wordList().ofOne();
      highCardinalityTerms = strings().wordList().multi(5);
      Docs docs =
          docs()
              .field("id", integers().incrementing())
              .field("numbers_i_dv", setValues)
              .field("numbers_i", setValues)
              .field("numbers_d", setValues)
              .field("numbers_d_dv", setValues)
              .field("numbers_l", setValues)
              .field("numbers_l_dv", setValues)
              .field("numbers_f", setValues)
              .field("numbers_f_dv", setValues)
              .field("term_low_s", lowCardinalityTerms)
              .field("term_high_s", highCardinalityTerms);
      // .field("numbers_dt", setValues);

      miniClusterState.index(COLLECTION, docs, numDocs, false);
      basePath = miniClusterState.nodes.get(0);
      SolrQuery q = new SolrQuery("*:*");
      q.setParam("facet", "true");
      q.setParam("rows", "0");
      q.setParam("facet.field", "numbers_i_dv", "term_low_s", "term_high_s");
      q.setParam("facet.limit", String.valueOf(maxCardinality));
      QueryRequest req = new QueryRequest(q);
      req.setBasePath(basePath);
      QueryResponse response = req.process(miniClusterState.client, COLLECTION);
      Set<String> numbers =
          response.getFacetField("numbers_i_dv").getValues().stream()
              .map(FacetField.Count::getName)
              .collect(Collectors.toSet());
      Set<String> lowCardinalityStrings =
          response.getFacetField("term_low_s").getValues().stream()
              .map(FacetField.Count::getName)
              .collect(Collectors.toSet());
      Set<String> highCardinalityStrings =
          response.getFacetField("term_high_s").getValues().stream()
              .map(FacetField.Count::getName)
              .collect(Collectors.toSet());

      lowCardTerms = new CircularIterator<>(lowCardinalityStrings);
      highCardTerms = new CircularIterator<>(highCardinalityStrings);
      queries = new CircularIterator<>(buildSetQueries(numbers));
    }

    private Set<String> buildSetQueries(Set<String> numbers) {
      Iterator<String> valueIterator = numbers.iterator();
      Set<String> setQueries = new HashSet<>();
      StringBuilder builder = new StringBuilder();
      while (valueIterator.hasNext()) {
        for (int i = 0; i < setQuerySize; i++) {
          if (!valueIterator.hasNext()) {
            break;
          }
          builder.append(valueIterator.next()).append(",");
        }
        builder.setLength(builder.length() - 1);
        setQueries.add(builder.toString());
        builder.setLength(0);
      }
      return setQueries;
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
      return setQuery("numbers_i" + (dvs ? "_dv" : ""));
    }

    public QueryRequest longSetQuery(boolean dvs) {
      return setQuery("numbers_l" + (dvs ? "_dv" : ""));
    }

    public QueryRequest doubleSetQuery(boolean dvs) {
      return setQuery("numbers_d" + (dvs ? "_dv" : ""));
    }

    public QueryRequest floatSetQuery(boolean dvs) {
      return setQuery("numbers_f" + (dvs ? "_dv" : ""));
    }

    QueryRequest setQuery(String field) {
      QueryRequest q =
          new QueryRequest(
              new SolrQuery(
                  "q",
                  termQueryField + ":" + lowCardTerms.next(),
                  "fq",
                  "{!terms cache=false f='" + field + "'}" + queries.next()));
      q.setBasePath(basePath);
      return q;
    }

    private static class CircularIterator<T> implements Iterator<T> {

      private final Object[] collection;
      private final AtomicInteger idx;

      CircularIterator(Collection<T> collection) {
        this.collection = Objects.requireNonNull(collection).toArray();
        if (this.collection.length == 0) {
          throw new IllegalArgumentException("This iterator doesn't support empty collections");
        }
        this.idx = new AtomicInteger();
      }

      @Override
      public boolean hasNext() {
        return true;
      }

      @SuppressWarnings("unchecked")
      @Override
      public T next() {
        return (T) collection[idx.incrementAndGet() % collection.length];
      }
    }
  }

  @Benchmark
  public Object intSet(
      Blackhole blackhole,
      BenchState benchState,
      MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response =
        benchState.intSetQuery(false).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object longSet(
      Blackhole blackhole,
      BenchState benchState,
      MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response =
        benchState.longSetQuery(false).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object floatSet(
      Blackhole blackhole,
      BenchState benchState,
      MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response =
        benchState.floatSetQuery(false).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object doubleSet(
      Blackhole blackhole,
      BenchState benchState,
      MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response =
        benchState.doubleSetQuery(false).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object intDvSet(
      Blackhole blackhole,
      BenchState benchState,
      MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response =
        benchState.intSetQuery(true).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object longDvSet(
      Blackhole blackhole,
      BenchState benchState,
      MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response =
        benchState.longSetQuery(true).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object floatDvSet(
      Blackhole blackhole,
      BenchState benchState,
      MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response =
        benchState.floatSetQuery(true).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }

  @Benchmark
  public Object doubleDvSet(
      Blackhole blackhole,
      BenchState benchState,
      MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    QueryResponse response =
        benchState.doubleSetQuery(true).process(miniClusterState.client, COLLECTION);
    blackhole.consume(response);
    return response;
  }
}

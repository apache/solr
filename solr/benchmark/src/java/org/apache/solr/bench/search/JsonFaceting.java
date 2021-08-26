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

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.DocMaker;
import org.apache.solr.bench.FieldDef;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

/** A benchmark to experiment with the performance of json faceting. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Warmup(time = 5, iterations = 3)
@Measurement(time = 15, iterations = 5)
@Fork(value = 1)
@Timeout(time = 60)
public class JsonFaceting {

  @State(Scope.Benchmark)
  public static class BenchState {

    public String collection = "testCollection";

    @Param({"100000"})
    public int docCount;

    @Param("2")
    int nodeCount;

    @Param("1")
    int numReplicas;

    @Param("2")
    int numShards;

    // DV,  // DocValues, collect into ordinal array
    // UIF, // UnInvertedField, collect into ordinal array
    // DVHASH, // DocValues, collect into hash
    // ENUM, // TermsEnum then intersect DocSet (stream-able)
    // STREAM, // presently equivalent to ENUM
    // SMART,
    //  "dv"
    //  "uif"
    //  "dvhash">
    //  "enum"
    //  "stream"
    //  "smart"
    @Param({"smart"})
    String fm;

    @Param({"5000"})
    int facetCard;

    @Param({"1000"})
    int facetCard2;

    private ModifiableSolrParams params;

    @Setup(Level.Trial)
    public void setup(
        BenchmarkParams benchmarkParams, MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {

      SplittableRandom random = miniClusterState.getRandom();

      System.setProperty("maxMergeAtOnce", "20");
      System.setProperty("segmentsPerTier", "20");

      miniClusterState.startMiniCluster(nodeCount);

      miniClusterState.createCollection(collection, numShards, numReplicas);

      // Define random documents
      DocMaker docMaker = new DocMaker();
      docMaker.addField(
          "id", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.UNIQUE_INT));
      docMaker.addField(
          "facet_s",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.ALPHEBETIC)
              .withMaxLength(64)
              .withMaxCardinality(facetCard, random));
      docMaker.addField(
          "facet2_s",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.ALPHEBETIC)
              .withMaxLength(16)
              .withMaxCardinality(facetCard, random));
      docMaker.addField(
          "facet3_s",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.UNICODE)
              .withMaxLength(128)
              .withMaxCardinality(facetCard2, random));
      docMaker.addField(
          "text",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.ALPHEBETIC)
              .withMaxLength(64)
              .withTokenCount(random.nextInt(350, 512)));
      docMaker.addField(
          "int_i", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.INTEGER));
      docMaker.addField(
          "int2_i",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.INTEGER)
              .withMaxCardinality(facetCard2, random));
      docMaker.addField(
          "int3_i",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.INTEGER)
              .withMaxCardinality(facetCard2, random));
      docMaker.addField(
          "int4_i",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.INTEGER)
              .withMaxCardinality(facetCard2, random));

      miniClusterState.index(collection, docMaker, docCount);
      miniClusterState.forceMerge(collection, 15);

      params = new ModifiableSolrParams();

      MiniClusterState.params(
          params,
          "q",
          "*:*",
          "json.facet",
          "{f1:{method:'"
              + fm
              + "', type:terms, field:'facet_s', sort:'x desc', facet:{x:'min(int3_i)'}  }"
              + " , f2:{method:'"
              + fm
              + "',, type:terms, field:'facet_s', sort:'x desc', facet:{x:'max(int3_i)'}  } "
              + " , f3:{method:'"
              + fm
              + "', type:terms, field:'facet_s', sort:'x desc', facet:{x:'unique(facet2_s)'}  } "
              + " , f4:{method:'"
              + fm
              + "', type:terms, field:'facet_s', sort:'x desc', facet:{x:'hll(facet2_s)'}  } "
              + " , f5:{method:'"
              + fm
              + "', type:terms, field:'facet_s', sort:'x desc', facet:{x:'variance(int3_i)'}  } "
              + " , f6:{type:terms, field:'int3_i', limit:1, sort:'x desc', facet:{x:'hll(int2_i)'}  } "
              + " , f7:{type:terms, field:'facet_s', limit:2, sort:'x desc', facet:{x:'missing(int4_i)'}  } "
              + " , f8:{type:terms, field:'facet_s', limit:2, sort:'x desc', facet:{x:'countvals(int4_i)'}  } "
              + "}");

      // MiniClusterState.log("params: " + params + "\n");
    }

    @State(Scope.Thread)
    public static class ThreadState {

      private SplittableRandom random;

      @Setup(Level.Trial)
      public void setup() throws Exception {
        Long seed = Long.getLong("randomSeed");

        this.random = new SplittableRandom(seed);
      }
    }
  }

  @Benchmark
  @Timeout(time = 500, timeUnit = TimeUnit.SECONDS)
  public Object jsonFacet(
      MiniClusterState.MiniClusterBenchState miniClusterState,
      BenchState state,
      BenchState.ThreadState threadState)
      throws Exception {
    QueryRequest queryRequest = new QueryRequest(state.params);
    queryRequest.setBasePath(
        miniClusterState.nodes.get(threadState.random.nextInt(state.nodeCount)));

    NamedList<Object> result = miniClusterState.client.request(queryRequest, state.collection);

    // MiniClusterState.log("result: " + result);

    return result;
  }
}

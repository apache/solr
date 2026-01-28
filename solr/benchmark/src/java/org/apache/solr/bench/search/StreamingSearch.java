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
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.bench.MiniClusterState.MiniClusterBenchState;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1)
@BenchmarkMode(Mode.Throughput)
@Warmup(time = 5, iterations = 1)
@Measurement(time = 30, iterations = 4)
@Threads(value = 1)
public class StreamingSearch {

  private static final String collection = "benchStreamingSearch";

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"false", "true"})
    boolean useHttp1;

    @Param("3")
    int nodeCount;

    @Param("3")
    int numShards;

    @Param("1")
    int numReplicas;

    @Param("1000")
    int docCount;

    @Param("4")
    int indexThreads; // 0 = sequential indexing

    @Param("500")
    int batchSize;

    @Param("1024")
    int docSizeBytes; // Target document size in bytes (approximate)

    @Param("3")
    int numTextFields; // Number of textN_ts fields to generate

    private String zkHost;
    private ModifiableSolrParams params;
    private StreamContext streamContext;
    private HttpJettySolrClient httpJettySolrClient;

    @Setup(Level.Trial)
    public void setup(MiniClusterBenchState miniClusterState) throws Exception {

      miniClusterState.startMiniCluster(nodeCount);
      miniClusterState.createCollection(collection, numShards, numReplicas);

      Docs docGen = createDocsWithTargetSize(docSizeBytes, numTextFields);

      if (indexThreads > 0) {
        miniClusterState.indexParallelBatched(
            collection, docGen, docCount, indexThreads, batchSize);
      } else {
        miniClusterState.index(collection, docGen, docCount, false);
      }
      miniClusterState.waitForMerges(collection);

      zkHost = miniClusterState.zkHost;

      // Build field list dynamically based on numTextFields
      StringBuilder flBuilder = new StringBuilder("id");
      for (int i = 1; i <= numTextFields; i++) {
        flBuilder.append(",text").append(i).append("_ts");
      }
      flBuilder.append(",int1_i_dv");

      params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.set(CommonParams.FL, flBuilder.toString());
      params.set(CommonParams.SORT, "id asc,int1_i_dv asc");
      params.set(CommonParams.ROWS, docCount);
    }

    /**
     * Creates a Docs generator that produces documents with approximately the target size.
     *
     * @param targetSizeBytes target document size in bytes (approximate)
     * @param numFields number of textN_ts fields to generate
     * @return Docs generator configured for the target size
     */
    private Docs createDocsWithTargetSize(int targetSizeBytes, int numFields) {
      // Calculate how many characters per field to approximate target size
      // Each character is ~1 byte in basic Latin alphabet
      // Account for field overhead, id field, and int field
      int baseOverhead = 100; // Approximate overhead for id, int field, and field names
      int availableBytes = Math.max(100, targetSizeBytes - baseOverhead);
      int bytesPerField = availableBytes / Math.max(1, numFields);

      // Use multi-value strings: multi(N) creates N strings joined by spaces
      // Calculate words and word length to hit target
      int wordsPerField = Math.max(1, bytesPerField / 50); // ~50 chars per word avg
      int wordLength = Math.min(64, Math.max(10, bytesPerField / Math.max(1, wordsPerField)));
      int minWordLength = Math.max(5, wordLength - 10);
      int maxWordLength = wordLength + 10;

      Docs docGen = docs().field("id", integers().incrementing());

      for (int i = 1; i <= numFields; i++) {
        docGen.field(
            "text" + i + "_ts",
            strings()
                .basicLatinAlphabet()
                .multi(wordsPerField)
                .ofLengthBetween(minWordLength, maxWordLength));
      }

      docGen.field("int1_i_dv", integers().all());
      return docGen;
    }

    @Setup(Level.Iteration)
    public void setupIteration(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws SolrServerException, IOException {
      SolrClientCache solrClientCache;
      // TODO tune params?
      var client = new HttpJettySolrClient.Builder().useHttp1_1(useHttp1).build();
      solrClientCache = new SolrClientCache(client);

      streamContext = new StreamContext();
      streamContext.setSolrClientCache(solrClientCache);
    }

    @TearDown(Level.Iteration)
    public void teardownIt() {
      streamContext.getSolrClientCache().close();
      if (httpJettySolrClient != null) {
        httpJettySolrClient.close();
      }
    }
  }

  @Benchmark
  public Object stream(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    CloudSolrStream stream = new CloudSolrStream(benchState.zkHost, collection, benchState.params);
    stream.setStreamContext(benchState.streamContext);
    return getTuples(stream);
  }

  private static List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<>();
    try {
      tupleStream.open();
      while (true) {
        Tuple t = tupleStream.read();
        if (t.EOF) {
          break;
        } else {
          tuples.add(t);
        }
      }
      return tuples;
    } finally {
      tupleStream.close();
    }
  }
}

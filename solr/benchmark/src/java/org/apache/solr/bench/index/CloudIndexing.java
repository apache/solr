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
import org.apache.solr.bench.Docs;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(16)
@Warmup(time = 15, iterations = 2)
@Measurement(time = 60, iterations = 4)
@Fork(value = 1)
// A benchmark to experiment with the performance of distributed indexing.
public class CloudIndexing {

  @State(Scope.Benchmark)
  public static class BenchState {

    static final String COLLECTION = "testCollection";

    @Param("4")
    int nodeCount;

    @Param("4")
    int numShards;

    @Param({"1", "3"})
    int numReplicas;

    @Param({"true", "false"})
    boolean useSmallDocs;

    @Param({"50000"})
    int preGenerate;

    private final Docs largeDocs;
    private final Docs smallDocs;
    private Iterator<SolrInputDocument> docIterator;

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
      smallDocs =
          docs()
              .field("id", integers().incrementing())
              .field("text", strings().basicLatinAlphabet().multi(2).ofLengthBetween(20, 32))
              .field("int1_i", integers().all())
              .field("int2_i", integers().all())
              .field("long1_l", longs().all());
    }

    private SolrInputDocument getNextDoc() {
      return docIterator.next();
    }

    private void preGenerate() {
      try {
        if (useSmallDocs) {
          smallDocs.preGenerate(preGenerate);
          docIterator = smallDocs.generatedDocsCircularIterator();
        } else {
          largeDocs.preGenerate(preGenerate);
          docIterator = largeDocs.generatedDocsCircularIterator();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Setup(Level.Trial)
    public void doSetup(MiniClusterState.MiniClusterBenchState miniClusterState) throws Exception {
      preGenerate();

      System.setProperty("mergePolicyFactory", "org.apache.solr.index.NoMergePolicyFactory");
      miniClusterState.startMiniCluster(nodeCount);
      miniClusterState.createCollection(COLLECTION, numShards, numReplicas);
    }
  }

  @Benchmark
  public Object indexDoc(MiniClusterState.MiniClusterBenchState miniClusterState, BenchState state)
      throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(
        miniClusterState.nodes.get(miniClusterState.getRandom().nextInt(state.nodeCount)));
    updateRequest.add(state.getNextDoc());
    return miniClusterState.client.request(updateRequest, BenchState.COLLECTION);
  }
}

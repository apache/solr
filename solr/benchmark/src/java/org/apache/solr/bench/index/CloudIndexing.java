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

import java.util.Iterator;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.DocMaker;
import org.apache.solr.bench.FieldDef;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(6)
@Warmup(time = 3, iterations = 5)
@Measurement(time = 15, iterations = 5)
@Fork(value = 1)
@Timeout(time = 60)
/** A benchmark to experiment with the performance of distributed indexing. */
public class CloudIndexing {

  @State(Scope.Benchmark)
  public static class BenchState {

    String collection = "testCollection";

    @Param("4")
    int nodeCount;

    @Param("5")
    int numShards;

    @Param({"1", "3", "9"})
    int numReplicas;

    @Param({"100000"})
    public int docCount;

    @Setup(Level.Iteration)
    public void doSetup(MiniClusterState.MiniClusterBenchState miniClusterState) throws Exception {

      miniClusterState.startMiniCluster(nodeCount);

      miniClusterState.createCollection(collection, numShards, numReplicas);
    }

    @TearDown(Level.Iteration)
    public void doTearDown(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      miniClusterState.shutdownMiniCluster();
    }

    @State(Scope.Thread)
    public static class Docs {

      private DocMaker largeDocMaker;
      private Iterator<SolrInputDocument> largeDocIterator;

      private DocMaker smallDocMaker;
      private Iterator<SolrInputDocument> smallDocIterator;

      private SplittableRandom random;

      @Setup(Level.Trial)
      public void setupDoc(BenchState state) throws Exception {
        Long seed = Long.getLong("randomSeed");

        random = new SplittableRandom(seed);

        largeDocMaker = new DocMaker();
        largeDocMaker.addField(
            "id", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.UNIQUE_INT));
        largeDocMaker.addField(
            "text",
            FieldDef.FieldDefBuilder.aFieldDef()
                .withContent(DocMaker.Content.ALPHEBETIC)
                .withMaxLength(64)
                .withTokenCount(random.nextInt(350, 512)));

        largeDocMaker.preGenerateDocs(state.docCount, random);

        largeDocIterator = largeDocMaker.getGeneratedDocsIterator();

        smallDocMaker = new DocMaker();
        smallDocMaker.addField(
            "id", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.UNIQUE_INT));
        smallDocMaker.addField(
            "text",
            FieldDef.FieldDefBuilder.aFieldDef()
                .withContent(DocMaker.Content.ALPHEBETIC)
                .withMaxLength(32)
                .withTokenCount(1));

        smallDocMaker.preGenerateDocs(state.docCount, random);

        smallDocIterator = smallDocMaker.getGeneratedDocsIterator();
      }

      public SolrInputDocument getLargeDoc() {
        if (!largeDocIterator.hasNext()) {
          largeDocIterator = largeDocMaker.getGeneratedDocsIterator();
        }
        return largeDocIterator.next();
      }

      public SolrInputDocument getSmallDoc() {
        if (!smallDocIterator.hasNext()) {
          smallDocIterator = smallDocMaker.getGeneratedDocsIterator();
        }
        return smallDocIterator.next();
      }
    }
  }

  @Benchmark
  @Timeout(time = 300)
  public Object indexLargeDoc(
      MiniClusterState.MiniClusterBenchState miniClusterState,
      BenchState state,
      BenchState.Docs docState)
      throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(miniClusterState.nodes.get(docState.random.nextInt(state.nodeCount)));
    SolrInputDocument doc = docState.getLargeDoc();

    updateRequest.add(doc);

    return miniClusterState.client.request(updateRequest, state.collection);
  }

  @Benchmark
  @Timeout(time = 300)
  public Object indexSmallDoc(
      MiniClusterState.MiniClusterBenchState miniClusterState,
      BenchState state,
      BenchState.Docs docState)
      throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(miniClusterState.nodes.get(docState.random.nextInt(state.nodeCount)));
    SolrInputDocument doc = docState.getSmallDoc();

    updateRequest.add(doc);

    return miniClusterState.client.request(updateRequest, state.collection);
  }
}

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

import org.apache.solr.bench.DocMaker;
import org.apache.solr.bench.FieldDef;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(6)
@Warmup(time = 3, iterations = 1)
@Measurement(time = 15, iterations = 1)
@Fork(value = 1)
@Timeout(time = 60)
/** A benchmark to experiment with the performance of distributed indexing. */
public class DistributedDeleteByQuery {

  @State(Scope.Benchmark)
  public static class BenchState {

    String collection = "testCollection";

    @Param("9")
    int nodeCount;

    @Param("3")
    int numShards;

    @Param({"3"})
    int numReplicas;

    @Param({"100000"})
    public int docCount;

    @Param({"1000"})
    public int uniqueDocCount;

    @Param({"100"})
    int batchSize;

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

      private DocMaker smallDocMaker;
      private Iterator<SolrInputDocument> smallDocIterator;

      private SplittableRandom random;

      @Setup(Level.Trial)
      public void setupDoc(BenchState state) throws Exception {
        Long seed = Long.getLong("randomSeed");

        random = new SplittableRandom(seed);

        smallDocMaker = new DocMaker();
        smallDocMaker.addField(
            "id", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.INTEGER)
                        .withMaxCardinality(state.uniqueDocCount, random));
        smallDocMaker.addField(
            "text",
            FieldDef.FieldDefBuilder.aFieldDef()
                .withContent(DocMaker.Content.ALPHEBETIC)
                .withMaxLength(64)
                .withTokenCount(random.nextInt(350, 512)));

        smallDocMaker.preGenerateDocs(state.docCount, random);

        smallDocIterator = smallDocMaker.getGeneratedDocsIterator();

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
  public Object deleteByQueryAndIndexBatch(
      MiniClusterState.MiniClusterBenchState miniClusterState,
      BenchState state,
      BenchState.Docs docState)
      throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(miniClusterState.nodes.get(docState.random.nextInt(state.nodeCount)));

    List<String> deleteIds = new ArrayList<>();

    for (int i = 0; i < state.batchSize; i++) {
      SolrInputDocument doc = docState.getSmallDoc();
      updateRequest.add(doc);
      deleteIds.add(doc.getFieldValue("id").toString());
    }

    UpdateRequest deleteRequest =  new UpdateRequest();
    deleteRequest.setBasePath(miniClusterState.nodes.get(docState.random.nextInt(state.nodeCount)));
    deleteRequest.deleteByQuery("id:(" + String.join(" ", deleteIds) + ")");

    miniClusterState.client.request(deleteRequest, state.collection);
    return miniClusterState.client.request(updateRequest, state.collection);
  }

}

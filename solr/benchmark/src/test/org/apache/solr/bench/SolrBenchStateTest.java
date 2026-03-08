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
package org.apache.solr.bench;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.booleans;
import static org.apache.solr.bench.generators.SourceDSL.dates;
import static org.apache.solr.bench.generators.SourceDSL.doubles;
import static org.apache.solr.bench.generators.SourceDSL.floats;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.longs;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.After;
import org.junit.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.WorkloadParams;
import org.openjdk.jmh.runner.options.TimeValue;

@ThreadLeakLingering(linger = 10)
public class SolrBenchStateTest extends SolrTestCaseJ4 {
  private SolrBenchState solrBenchState;
  private BaseBenchState baseBenchState;
  private BenchmarkParams benchParams;

  @Test
  public void testMiniClusterState() throws Exception {

    System.setProperty("workBaseDir", createTempDir("work").toString());
    System.setProperty("random.counts", "true");

    solrBenchState = new SolrBenchState();
    benchParams =
        new BenchmarkParams(
            "benchmark",
            "generatedTarget",
            true,
            1,
            new int[] {1},
            Collections.singletonList("label"),
            0,
            0,
            new IterationParams(IterationType.WARMUP, 1, TimeValue.milliseconds(10), 1),
            new IterationParams(IterationType.MEASUREMENT, 1, TimeValue.milliseconds(10), 1),
            Mode.Throughput,
            new WorkloadParams(),
            TimeUnit.SECONDS,
            1,
            "jvm",
            Collections.singletonList("jvmArg"),
            "jdkVersion",
            "vmName",
            "vmVersion",
            "jmhVersion",
            TimeValue.seconds(10));
    baseBenchState = new BaseBenchState();
    baseBenchState.doSetup(benchParams);
    solrBenchState.doSetup(benchParams, baseBenchState);

    int nodeCount = 3;
    solrBenchState.startMiniCluster(nodeCount);
    String collection = "collection1";
    int numShards = 1;
    int numReplicas = 1;
    solrBenchState.createCollection(collection, numShards, numReplicas);

    Docs docs =
        docs()
            .field("id", integers().incrementing())
            .field(strings().basicLatinAlphabet().multi(312).ofLengthBetween(30, 64))
            .field(strings().basicLatinAlphabet().multi(312).ofLengthBetween(30, 64))
            .field(integers().all())
            .field(integers().all())
            .field(integers().all())
            .field(longs().all())
            .field(longs().all())
            .field(floats().all())
            .field(floats().all())
            .field(booleans().all())
            .field(booleans().all())
            .field(dates().all())
            .field(dates().all())
            .field(doubles().all())
            .field(doubles().all());

    int numDocs = 50;
    docs.preGenerate(numDocs);

    solrBenchState.index(collection, docs, numDocs);

    solrBenchState.forceMerge(collection, 15);

    ModifiableSolrParams params = SolrBenchState.params("q", "*:*");
    QueryRequest queryRequest = new QueryRequest(params);
    QueryResponse result = queryRequest.process(solrBenchState.client, collection);

    BaseBenchState.log("match all query result=" + result);

    assertEquals(numDocs, result.getResults().getNumFound());
  }

  @After
  public void after() throws Exception {
    BaseBenchState.doTearDown(benchParams);

    solrBenchState.tearDown(benchParams);
    solrBenchState.shutdownMiniCluster(benchParams, baseBenchState);
  }
}

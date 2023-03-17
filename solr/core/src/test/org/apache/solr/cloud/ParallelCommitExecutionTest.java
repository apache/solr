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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParallelCommitExecutionTest extends SolrCloudTestCase {

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  /** A collection specific client for operations at the cloud level */
  private static CloudSolrClient COLLECTION_CLIENT;

  private static int expectCount;

  private static volatile CountDownLatch countdown;
  private static final AtomicInteger countup = new AtomicInteger();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // multi replicas matters; for the initial parallel commit execution tests, only consider
    // repFactor=1
    final int repFactor = 1; // random().nextBoolean() ? 1 : 2;
    final int numShards = TestUtil.nextInt(random(), 1, 4);
    final int numNodes = (numShards * repFactor);
    expectCount = numNodes;

    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");

    configureCluster(numNodes).addConfig(configName, configDir).configure();

    Map<String, String> collectionProperties = new LinkedHashMap<>();
    collectionProperties.put("config", "solrconfig-parallel-commit.xml");
    collectionProperties.put("schema", "schema_latest.xml");
    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    COLLECTION_CLIENT = cluster.getSolrClient(COLLECTION_NAME);
    waitForRecoveriesToFinish(COLLECTION_CLIENT);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (null != COLLECTION_CLIENT) {
      COLLECTION_CLIENT.close();
      COLLECTION_CLIENT = null;
    }
  }

  private static void initSyncVars() {
    final int ct;
    ct = expectCount;
    countdown = new CountDownLatch(ct);
    countup.set(0);
  }

  @Test
  public void testParallelOk() throws Exception {
    initSyncVars();
    COLLECTION_CLIENT.commit(true, true);
    assertEquals(0, countdown.getCount());
    assertEquals(expectCount, countup.get());
  }

  public static void waitForRecoveriesToFinish(CloudSolrClient client) throws Exception {
    assertNotNull(client.getDefaultCollection());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        client.getDefaultCollection(), ZkStateReader.from(client), true, true, 330);
  }

  public static class CheckFactory extends UpdateRequestProcessorFactory {
    @Override
    public UpdateRequestProcessor getInstance(
        SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return new Check(next);
    }
  }

  public static class Check extends UpdateRequestProcessor {

    public Check(UpdateRequestProcessor next) {
      super(next);
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      super.processCommit(cmd);
      countdown.countDown();
      try {
        // NOTE: this ensures that all commits are executed in parallel; no commit can complete
        // successfully until all commits have entered the `processCommit(...)` method.
        if (!countdown.await(5, TimeUnit.SECONDS)) {
          throw new RuntimeException("done waiting");
        }
        countup.incrementAndGet();
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}

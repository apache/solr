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
package org.apache.solr.handler.component;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests specifying a custom ShardHandlerFactory */
public class TestHttpShardHandlerFactory extends SolrTestCaseJ4 {

  private static final String LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE =
      "solr.tests.loadBalancerRequestsMinimumAbsolute";
  private static final String LOAD_BALANCER_REQUESTS_MAX_FRACTION =
      "solr.tests.loadBalancerRequestsMaximumFraction";

  private static int expectedLoadBalancerRequestsMinimumAbsolute = 0;
  private static float expectedLoadBalancerRequestsMaximumFraction = 1.0f;

  @BeforeClass
  public static void beforeTests() {
    expectedLoadBalancerRequestsMinimumAbsolute = random().nextInt(3); // 0 .. 2
    expectedLoadBalancerRequestsMaximumFraction = (1 + random().nextInt(10)) / 10f; // 0.1 .. 1.0
    System.setProperty(
        LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE,
        Integer.toString(expectedLoadBalancerRequestsMinimumAbsolute));
    System.setProperty(
        LOAD_BALANCER_REQUESTS_MAX_FRACTION,
        Float.toString(expectedLoadBalancerRequestsMaximumFraction));
  }

  @AfterClass
  public static void afterTests() {
    System.clearProperty(LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE);
    System.clearProperty(LOAD_BALANCER_REQUESTS_MAX_FRACTION);
  }

  public void testLoadBalancerRequestsMinMax() {
    final Path home = TEST_PATH();
    CoreContainer cc = null;
    ShardHandlerFactory factory = null;
    try {
      cc =
          CoreContainer.createAndLoad(
              home, home.resolve("solr-shardhandler-loadBalancerRequests.xml"));
      factory = cc.getShardHandlerFactory();

      // test that factory is HttpShardHandlerFactory with expected url reserve fraction
      assertTrue(factory instanceof HttpShardHandlerFactory);
      @SuppressWarnings("resource")
      final HttpShardHandlerFactory httpShardHandlerFactory = ((HttpShardHandlerFactory) factory);
      assertEquals(
          expectedLoadBalancerRequestsMinimumAbsolute,
          httpShardHandlerFactory.permittedLoadBalancerRequestsMinimumAbsolute,
          0.0);
      assertEquals(
          expectedLoadBalancerRequestsMaximumFraction,
          httpShardHandlerFactory.permittedLoadBalancerRequestsMaximumFraction,
          0.0);

      // create a dummy request and dummy url list
      final QueryRequest queryRequest = null;
      final List<String> urls = new ArrayList<>();
      for (int ii = 0; ii < 10; ++ii) {
        urls.add("http://localhost" + ii + ":8983/solr");
      }

      // create LBHttpSolrClient request
      final LBSolrClient.Req req =
          httpShardHandlerFactory.newLBHttpSolrClientReq(queryRequest, urls);

      // actual vs. expected test
      final int actualNumServersToTry = req.getNumServersToTry();
      int expectedNumServersToTry =
          (int) Math.floor(urls.size() * expectedLoadBalancerRequestsMaximumFraction);
      if (expectedNumServersToTry < expectedLoadBalancerRequestsMinimumAbsolute) {
        expectedNumServersToTry = expectedLoadBalancerRequestsMinimumAbsolute;
      }
      assertEquals(
          "wrong numServersToTry for"
              + " urls.size="
              + urls.size()
              + " expectedLoadBalancerRequestsMinimumAbsolute="
              + expectedLoadBalancerRequestsMinimumAbsolute
              + " expectedLoadBalancerRequestsMaximumFraction="
              + expectedLoadBalancerRequestsMaximumFraction,
          expectedNumServersToTry,
          actualNumServersToTry);

    } finally {
      if (factory != null) factory.close();
      if (cc != null) cc.shutdown();
    }
  }

  @Test
  public void getShardsAllowList() {
    System.setProperty(TEST_URL_ALLOW_LIST, "http://abc:8983/,http://def:8984/,");
    CoreContainer cc = null;
    ShardHandlerFactory factory = null;
    try {
      final Path home = TEST_PATH();
      cc = CoreContainer.createAndLoad(home, home.resolve("solr.xml"));
      factory = cc.getShardHandlerFactory();
      assertTrue(factory instanceof HttpShardHandlerFactory);
      assertThat(
          cc.getAllowListUrlChecker().getHostAllowList(),
          equalTo(new HashSet<>(Arrays.asList("abc:8983", "def:8984"))));
    } finally {
      if (factory != null) factory.close();
      if (cc != null) cc.shutdown();
      System.clearProperty(TEST_URL_ALLOW_LIST);
    }
  }

  @Test
  public void testLiveNodesToHostUrl() {
    Set<String> liveNodes =
        new HashSet<>(Arrays.asList("1.2.3.4:8983_solr", "1.2.3.4:9000_", "1.2.3.4:9001_solr-2"));
    ClusterState cs = new ClusterState(liveNodes, new HashMap<>());
    Set<String> hostSet = cs.getHostAllowList();
    assertThat(hostSet.size(), is(3));
    assertThat(hostSet, hasItem("1.2.3.4:8983"));
    assertThat(hostSet, hasItem("1.2.3.4:9000"));
    assertThat(hostSet, hasItem("1.2.3.4:9001"));
  }

  public void testHttpShardHandlerWithResponse() {
    HttpShardHandlerFactory httpShardHandlerFactory = new HttpShardHandlerFactory();
    HttpShardHandler shardHandler = (HttpShardHandler) httpShardHandlerFactory.getShardHandler();

    long timeAllowedInMillis = -1;
    // setting one pending request.
    shardHandler.setPendingRequest(1);

    ShardResponse shardResponse = new ShardResponse();
    shardResponse.setShard("shard_1");
    ShardRequest shardRequest = new ShardRequest();
    // one shard
    shardRequest.actualShards = new String[] {"shard_1"};
    shardResponse.setShardRequest(shardRequest);

    ExecutorService exec = ExecutorUtil.newMDCAwareCachedThreadPool("timeAllowedTest");
    try {
      // generating shardresponse for one shard
      exec.submit(() -> shardHandler.setResponse(shardResponse));
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(exec);
    }
    ShardResponse gotResponse =
            shardHandler.takeCompletedIncludingErrorsWithTimeout(timeAllowedInMillis);

    assertEquals(shardResponse, gotResponse);
  }

  @Test
  public void testHttpShardHandlerWithPartialResponse() {
    HttpShardHandlerFactory httpShardHandlerFactory = new HttpShardHandlerFactory();
    HttpShardHandler shardHandler = (HttpShardHandler) httpShardHandlerFactory.getShardHandler();

    long timeAllowedInMillis = 100;
    // setting two pending requests.
    shardHandler.setPendingRequest(2);

    ShardResponse shardResponse = new ShardResponse();
    shardResponse.setShard("shard_1");
    ShardRequest shardRequest = new ShardRequest();
    // two shards
    shardRequest.actualShards = new String[] {"shard_1", "shard_2"};
    shardResponse.setShardRequest(shardRequest);

    ExecutorService exec = ExecutorUtil.newMDCAwareCachedThreadPool("timeAllowedTest");
    try {
      // generating shardresponse for one shard only
      exec.submit(() -> shardHandler.setResponse(shardResponse));
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(exec);
    }

    // partial response
    ShardResponse gotResponse =
            shardHandler.takeCompletedIncludingErrorsWithTimeout(timeAllowedInMillis);

    assertEquals(shardResponse, gotResponse);
  }
}

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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.MockShardHandlerFactory;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests specifying a custom ShardHandlerFactory */
public class TestHttpShardHandlerFactory extends SolrTestCaseJ4 {

  private static final String[] SHARD_HANDLER_FACTORY_IMPLEMENTATIONS =
      new String[] {
        HttpShardHandlerFactory.class.getName(), ParallelHttpShardHandlerFactory.class.getName()
      };

  private static final String LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE =
      "solr.tests.loadBalancerRequestsMinimumAbsolute";
  private static final String LOAD_BALANCER_REQUESTS_MAX_FRACTION =
      "solr.tests.loadBalancerRequestsMaximumFraction";
  private static final String SHARD_HANDLER_FACTORY_PROPERTY =
      "solr.tests.defaultShardHandlerFactory";

  private static int expectedLoadBalancerRequestsMinimumAbsolute = 0;
  private static float expectedLoadBalancerRequestsMaximumFraction = 1.0f;
  private static String expectedShardHandlerFactory;

  @BeforeClass
  public static void beforeTests() {
    expectedLoadBalancerRequestsMinimumAbsolute = random().nextInt(3); // 0 .. 2
    expectedLoadBalancerRequestsMaximumFraction = (1 + random().nextInt(10)) / 10f; // 0.1 .. 1.0
    expectedShardHandlerFactory =
        SHARD_HANDLER_FACTORY_IMPLEMENTATIONS[
            random().nextInt(SHARD_HANDLER_FACTORY_IMPLEMENTATIONS.length)];
    System.setProperty(
        LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE,
        Integer.toString(expectedLoadBalancerRequestsMinimumAbsolute));
    System.setProperty(
        LOAD_BALANCER_REQUESTS_MAX_FRACTION,
        Float.toString(expectedLoadBalancerRequestsMaximumFraction));
    System.setProperty(SHARD_HANDLER_FACTORY_PROPERTY, expectedShardHandlerFactory);
  }

  @Test
  public void testLoadBalancerRequestsMinMax() throws ClassNotFoundException {
    final Path home = TEST_PATH();
    CoreContainer cc = null;
    ShardHandlerFactory factory = null;
    try {
      cc =
          CoreContainer.createAndLoad(
              home, home.resolve("solr-shardhandler-loadBalancerRequests.xml"));
      factory = cc.getShardHandlerFactory();

      assertThat(factory, instanceOf(Class.forName(expectedShardHandlerFactory)));
      // All SHF's currently extend HttpShardFactory, so this case is safe
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

  @Test
  public void testXML() {
    Path home = TEST_PATH();
    CoreContainer cc = CoreContainer.createAndLoad(home, home.resolve("solr-shardhandler.xml"));
    ShardHandlerFactory factory = cc.getShardHandlerFactory();
    assertTrue(factory instanceof MockShardHandlerFactory);
    NamedList<?> args = ((MockShardHandlerFactory) factory).getArgs();
    assertEquals("myMagicRequiredValue", args.get("myMagicRequiredParameter"));
    factory.close();
    cc.shutdown();
  }

  /** Test {@link ShardHandler#setShardAttributesToParams} */
  @Test
  public void testSetShardAttributesToParams() {
    // NOTE: the value of this test is really questionable; we should feel free to remove it
    ModifiableSolrParams modifiable = new ModifiableSolrParams();
    var dummyIndent = "Dummy-Indent";

    modifiable.set(ShardParams.SHARDS, "dummyValue");
    modifiable.set(CommonParams.HEADER_ECHO_PARAMS, "dummyValue");
    modifiable.set(CommonParams.INDENT, dummyIndent);

    ShardHandler.setShardAttributesToParams(modifiable, 2);

    assertEquals(Boolean.FALSE.toString(), modifiable.get(CommonParams.DISTRIB));
    assertEquals("2", modifiable.get(ShardParams.SHARDS_PURPOSE));
    assertEquals(Boolean.FALSE.toString(), modifiable.get(CommonParams.OMIT_HEADER));
    assertEquals(Boolean.TRUE.toString(), modifiable.get(ShardParams.IS_SHARD));

    assertNull(modifiable.get(CommonParams.HEADER_ECHO_PARAMS));
    assertNull(modifiable.get(ShardParams.SHARDS));
    assertNull(modifiable.get(CommonParams.INDENT));
  }
}

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

package org.apache.solr.update.processor;

import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.ROUTER_START;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.impl.BaseHttpClusterStateProvider;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests date math in the start parameter of a time routed alias: after a document is added, the
 * stored start is rewritten without date math so it parses as an instant.
 */
public class TimeRoutedAliasDateMathInStartTest extends RoutedAliasUpdateProcessorTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String alias = "myalias";
  private static final String timeField = "timestamp_dt";

  private CloudSolrClient solrClient;

  @Before
  public void doBefore() throws Exception {
    configureCluster(4).configure();
    solrClient = cluster.getSolrClient();
    // log this to help debug potential causes of problems
    if (log.isInfoEnabled()) {
      log.info("SolrClient: {}", solrClient);
      log.info("ClusterStateProvider {}", solrClient.getClusterStateProvider()); // nowarn
    }
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public CloudSolrClient getSolrClient() {
    return solrClient;
  }

  private String getTimeField() {
    return timeField;
  }

  @Test
  public void testDateMathInStart() throws Exception {
    ClusterStateProvider clusterStateProvider = solrClient.getClusterStateProvider();
    Class<? extends ClusterStateProvider> aClass = clusterStateProvider.getClass();
    System.out.println("CSPROVIDER:" + aClass);

    // This test prevents recurrence of SOLR-13760

    String configName = getSaferTestName();
    createConfigSet(configName);
    CountDownLatch aliasUpdate = new CountDownLatch(1);
    monitorAlias(aliasUpdate);

    // each collection has 4 shards with 3 replicas for 12 possible destinations
    // 4 of which are leaders, and 8 of which should fail this test.
    final int numShards = 1 + random().nextInt(4);
    final int numReplicas = 1 + random().nextInt(3);
    CollectionAdminRequest.createTimeRoutedAlias(
            alias,
            "2019-09-14T03:00:00Z/DAY",
            "+1DAY",
            getTimeField(),
            CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas))
        .process(solrClient);

    aliasUpdate.await();
    if (BaseHttpClusterStateProvider.class.isAssignableFrom(aClass)) {
      ((BaseHttpClusterStateProvider) clusterStateProvider).resolveAlias(getAlias(), true);
    }

    ModifiableSolrParams params = params();
    String nowDay =
        DateTimeFormatter.ISO_INSTANT.format(
            DateMathParser.parseMath(new Date(), "2019-09-14T01:00:00Z").toInstant());
    assertUpdateResponse(
        add(
            alias,
            Arrays.asList(
                sdoc(
                    "id",
                    "1",
                    "timestamp_dt",
                    nowDay)), // should not cause preemptive creation of 10-28 now
            params));

    // this process should have lead to the modification of the start time for the alias, converting
    // it into a parsable date, removing the DateMath

    // what we test next happens in a separate thread, so we have to give it some time to happen.
    // Our own watcher on /aliases.json may fire before the provider's ZkStateReader has refreshed
    // its aliases, so poll the provider itself instead of waiting on a latch.
    final TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    String hopeFullyModified;
    while (true) {
      if (BaseHttpClusterStateProvider.class.isAssignableFrom(aClass)) {
        ((BaseHttpClusterStateProvider) clusterStateProvider).resolveAlias(getAlias(), true);
      }
      hopeFullyModified = clusterStateProvider.getAliasProperties(getAlias()).get(ROUTER_START);
      if (hopeFullyModified != null) {
        try {
          Instant.parse(hopeFullyModified);
          break;
        } catch (DateTimeParseException e) {
          // still has date math, try again
        }
      }
      if (timeOut.hasTimedOut()) {
        fail(
            ROUTER_START
                + " should not have any date math by this point and parse as an instant. Using "
                + aClass
                + " Found:"
                + hopeFullyModified);
      }
      timeOut.sleep(100);
    }
  }

  private void monitorAlias(CountDownLatch aliasUpdate)
      throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    zkClient()
        .getData(
            "/aliases.json",
            new Watcher() {
              @Override
              public void process(WatchedEvent watchedEvent) {
                aliasUpdate.countDown();
              }
            },
            stat);
  }
}

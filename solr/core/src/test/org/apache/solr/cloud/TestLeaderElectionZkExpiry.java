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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.tests.util.LuceneTestCase.BadApple;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-16122")
public class TestLeaderElectionZkExpiry extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SOLRXML = "<solr></solr>";

  @Test
  public void testLeaderElectionWithZkExpiry() throws Exception {
    Path zkDir = createTempDir("zkData");
    Path ccDir = createTempDir("testLeaderElectionWithZkExpiry-solr");

    final ZkTestServer server = new ZkTestServer(zkDir);
    server.setTheTickTime(1000);
    try {
      server.run();

      CloudConfig cloudConfig =
          new CloudConfig.CloudConfigBuilder("dummy.host.com", 8984, "solr")
              .setLeaderConflictResolveWait(180000)
              .setLeaderVoteWait(180000)
              .build();

      CoreContainer cc = createCoreContainer(ccDir, SOLRXML);
      try {
        ExecutorService threadExecutor =
            ExecutorUtil.newMDCAwareSingleThreadExecutor(
                new SolrNamedThreadFactory(this.getTestName()));
        try (ZkController zkController =
            new ZkController(
                cc, server.getZkAddress(), 15000, cloudConfig, Collections::emptyList)) {
          threadExecutor.submit(
              () -> {
                TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
                while (!timeout.hasTimedOut()) {
                  server.expire(zkController.getZkClient().getZooKeeper().getSessionId());
                  try {
                    timeout.sleep(10);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              });
          try (SolrZkClient zc =
              new SolrZkClient.Builder()
                  .withUrl(server.getZkAddress())
                  .withTimeout(LeaderElectionTest.TIMEOUT, TimeUnit.MILLISECONDS)
                  .build()) {
            boolean found = false;
            TimeOut timeout = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
            while (!timeout.hasTimedOut()) {
              try {
                String leaderNode = OverseerCollectionConfigSetProcessor.getLeaderNode(zc);
                if (leaderNode != null && !leaderNode.trim().isEmpty()) {
                  if (log.isInfoEnabled()) {
                    log.info("Time={} Overseer leader is = {}", System.nanoTime(), leaderNode);
                  }
                  found = true;
                  break;
                }
              } catch (KeeperException.NoNodeException nne) {
                // ignore
              }
            }
            assertTrue(found);
          }
        } finally {
          ExecutorUtil.shutdownNowAndAwaitTermination(threadExecutor);
        }
      } finally {
        cc.shutdown();
      }
    } finally {
      server.shutdown();
    }
  }
}

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
package org.apache.solr.core;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;

/** Tests TLS initialization before the startup ZooKeeper connection check. */
@ThreadLeakLingering(linger = NodeConfigZookeeperTlsTest.NETTY_SHUTDOWN_DEADLINE_MS)
public class NodeConfigZookeeperTlsTest extends SolrTestCase {
  static final int NETTY_SHUTDOWN_DEADLINE_MS = 5000;
  private static final int CONNECT_TIMEOUT_SECONDS = 1;
  private static final long SERVER_DEADLINE_MS =
      TimeUnit.SECONDS.toMillis(CONNECT_TIMEOUT_SECONDS) * 10;

  public void testPreflightConnectionUsesTls() throws Exception {
    System.setProperty("zookeeper.client.secure", "true");
    System.setProperty("solr.cloud.wait.for.zk.seconds", Integer.toString(CONNECT_TIMEOUT_SECONDS));
    var executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("zk-preflight-tls"));
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
      server.setSoTimeout((int) SERVER_DEADLINE_MS);
      var firstByte =
          executor.submit(
              () -> {
                try (var peer = server.accept()) {
                  peer.setSoTimeout((int) SERVER_DEADLINE_MS);
                  return peer.getInputStream().read();
                }
              });
      Properties properties = new Properties();
      properties.setProperty("zkHost", "127.0.0.1:" + server.getLocalPort());
      Path home = createTempDir();
      SolrException failure =
          expectThrows(SolrException.class, () -> NodeConfig.loadNodeConfig(home, properties));
      assertTrue(failure.toString(), failure.getMessage().contains("testing zookeeper connection"));
      assertEquals(
          "The first packet must be a TLS handshake",
          22,
          firstByte.get(SERVER_DEADLINE_MS, TimeUnit.MILLISECONDS).intValue());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(SERVER_DEADLINE_MS, TimeUnit.MILLISECONDS));
    }
  }
}

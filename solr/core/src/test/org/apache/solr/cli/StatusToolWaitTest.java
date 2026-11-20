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

package org.apache.solr.cli;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** {@link StatusTool#waitForSolrUp} against ports that never answer a request. */
public class StatusToolWaitTest extends SolrTestCase {

  private final StatusTool tool = new StatusTool(new CLITestHelper.TestingRuntime(false));

  @Test
  public void listeningButUnreachableCountsAsUp() throws Exception {
    // Accepts connections and closes them at once, as a node rejecting this host's TLS handshake.
    ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    Thread acceptor =
        new Thread(
            () -> {
              while (!server.isClosed()) {
                try {
                  Socket socket = server.accept();
                  socket.close();
                } catch (IOException e) {
                  return;
                }
              }
            });
    acceptor.start();
    try {
      assertTrue(tool.waitForSolrUp("https://127.0.0.1:" + server.getLocalPort() + "/solr", 30));
    } finally {
      server.close();
      acceptor.join();
    }
  }

  @Test
  public void notListeningTimesOut() throws Exception {
    int port;
    try (ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      port = server.getLocalPort();
    }
    assertFalse(tool.waitForSolrUp("http://127.0.0.1:" + port + "/solr", 3));
  }
}

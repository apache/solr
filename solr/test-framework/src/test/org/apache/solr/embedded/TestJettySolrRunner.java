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
package org.apache.solr.embedded;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.junit.Test;

public class TestJettySolrRunner extends SolrTestCaseJ4 {

  @Test
  public void testPassSolrHomeToRunner() throws Exception {

    // We set a non-standard coreRootDirectory, create a core, and check that it has been
    // built in the correct place

    Path solrHome = createTempDir();
    Path coresDir = createTempDir("crazy_path_to_cores");

    Path configsets = TEST_PATH().resolve("configsets");

    String solrxml =
        "<solr><str name=\"configSetBaseDir\">CONFIGSETS</str><str name=\"coreRootDirectory\">COREROOT</str></solr>"
            .replace("CONFIGSETS", configsets.toString())
            .replace("COREROOT", coresDir.toString());
    Files.write(solrHome.resolve("solr.xml"), solrxml.getBytes(StandardCharsets.UTF_8));

    JettyConfig jettyConfig = JettyConfig.builder().build();

    JettySolrRunner runner =
        new JettySolrRunner(solrHome.toString(), new Properties(), jettyConfig);
    try {
      runner.start();

      SolrClient client = runner.getSolrClient();
      CoreAdminRequest.Create createReq = new CoreAdminRequest.Create();
      createReq.setCoreName("newcore");
      createReq.setConfigSet("minimal");

      client.request(createReq);

      assertTrue(Files.exists(coresDir.resolve("newcore").resolve("core.properties")));

    } finally {
      runner.stop();
    }
  }

  @Test
  public void testLookForBindException() throws IOException {
    Path solrHome = createTempDir();
    Files.write(
        solrHome.resolve("solr.xml"),
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.getBytes(Charset.defaultCharset()));

    JettyConfig config = JettyConfig.builder().build();

    JettySolrRunner jetty = new JettySolrRunner(solrHome.toString(), config);

    Exception result;
    BindException be = new BindException();
    IOException test = new IOException();

    result = jetty.lookForBindException(test);
    assertEquals(result, test);

    test = new IOException();
    result = jetty.lookForBindException(test);
    assertEquals(result, test);

    test = new IOException((Throwable) null);
    result = jetty.lookForBindException(test);
    assertEquals(result, test);

    test =
        new IOException() {
          @Override
          public synchronized Throwable getCause() {
            return this;
          }
        };
    result = jetty.lookForBindException(test);
    assertEquals(result, test);

    test = new IOException(new RuntimeException());
    result = jetty.lookForBindException(test);
    assertEquals(result, test);

    test = new IOException(new RuntimeException(be));
    result = jetty.lookForBindException(test);
    assertEquals(result, be);
  }

  @Test
  public void testStoppedRunnerKeepsItsPortUntilRestart() throws Exception {
    Path solrHome = createTempDir();
    Files.write(
        solrHome.resolve("solr.xml"),
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.getBytes(Charset.defaultCharset()));

    JettyConfig config = JettyConfig.builder().build();
    JettySolrRunner runner = new JettySolrRunner(solrHome.toString(), config);

    boolean running = false;
    try {
      runner.start();
      running = true;
      int port = runner.getLocalPort();

      runner.stop();
      running = false;

      // The framework holds the stopped runner's port, so a foreign process cannot take
      // it during the restart gap. This uses only the long-standing public API, so the
      // test also runs against the pre-fix framework, where the bind below succeeds.
      try (ServerSocket foreign = new ServerSocket()) {
        foreign.setReuseAddress(false);
        foreign.bind(new InetSocketAddress("127.0.0.1", port));
        fail("the stopped runner's port should still be reserved");
      } catch (BindException expected) {
        // the reservation is doing its job
      }

      // Restarting on the same port still works.
      runner.start();
      running = true;
      assertEquals(port, runner.getLocalPort());

      // Closing a stopped runner gives the port back. close() ends the runner's life,
      // so nothing keeps the reservation afterwards, unlike stop(), which is one half
      // of the stop and restart cycle.
      runner.stop();
      running = false;
      runner.close();
      try (ServerSocket foreign = new ServerSocket()) {
        foreign.setReuseAddress(false);
        foreign.bind(new InetSocketAddress("127.0.0.1", port));
        // the bind succeeded, so the reservation is gone
      } catch (BindException e) {
        fail("close() should release the stopped runner's port reservation");
      }
    } finally {
      if (running) {
        runner.stop();
      }
    }
  }

  @Test
  public void testStoppedRunnerThatServedTrafficKeepsItsPortUntilRestart() throws Exception {
    Path solrHome = createTempDir();
    Files.write(
        solrHome.resolve("solr.xml"),
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.getBytes(Charset.defaultCharset()));

    JettyConfig config = JettyConfig.builder().build();
    JettySolrRunner runner = new JettySolrRunner(solrHome.toString(), config);

    boolean running = false;
    try {
      runner.start();
      running = true;
      int port = runner.getLocalPort();

      // Serve a real request before the stop, through a client of this test rather than
      // the runner's own, which stop() closes. Connections a node served can leave the
      // port in TIME_WAIT once it stops, which the reservation bind has to tolerate (it
      // binds with address reuse, like the server connectors) while still holding the
      // port. The plain stop in the test above never serves traffic, so it cannot reach
      // this case.
      try (HttpJettySolrClient client =
          new HttpJettySolrClient.Builder(runner.getBaseUrl().toString()).build()) {
        new CoresApi.GetAllCoreStatus().process(client);
      }

      runner.stop();
      running = false;

      // A foreign bind with address reuse could take over connection sockets lingering
      // in TIME_WAIT, but it must still fail against the socket the framework itself
      // holds on the port, with or without reuse.
      for (boolean reuse : new boolean[] {false, true}) {
        try (ServerSocket foreign = new ServerSocket()) {
          foreign.setReuseAddress(reuse);
          foreign.bind(new InetSocketAddress("127.0.0.1", port));
          fail("the stopped runner's port should still be reserved (reuseAddress=" + reuse + ")");
        } catch (BindException expected) {
          // the reservation is doing its job
        }
      }

      // Restarting on the same port still works, and the runner serves again.
      runner.start();
      running = true;
      assertEquals(port, runner.getLocalPort());
      try (HttpJettySolrClient client =
          new HttpJettySolrClient.Builder(runner.getBaseUrl().toString()).build()) {
        new CoresApi.GetAllCoreStatus().process(client);
      }
    } finally {
      if (running) {
        runner.stop();
      }
    }
  }
}

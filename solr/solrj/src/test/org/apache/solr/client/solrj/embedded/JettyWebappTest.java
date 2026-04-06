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
package org.apache.solr.client.solrj.embedded;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Random;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.ExternalPaths;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.session.DefaultSessionIdManager;

/**
 * @since solr 1.3
 */
public class JettyWebappTest extends SolrTestCaseJ4 {
  int port = 0;

  Server server;
  HttpClient httpClient;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.solr.home", createTempDir().toString());
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
    System.setProperty("solr.tests.doContainerStreamCloseAssert", "false");

    Path dataDir = createTempDir();
    Files.createDirectories(dataDir);

    System.setProperty("solr.data.dir", dataDir.toRealPath().toString());
    String path = ExternalPaths.WEBAPP_HOME.toString();

    server = new Server(port);
    // insecure: only use for tests!!!!
    server.addBean(new DefaultSessionIdManager(server, new Random(random().nextLong())));
    server.setHandler(new WebAppContext(path, "/solr"));

    ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory());
    connector.setIdleTimeout(1000 * 60 * 60);
    connector.setPort(0);
    server.setConnectors(new Connector[] {connector});
    server.setStopAtShutdown(true);

    server.start();
    port = connector.getLocalPort();
    httpClient = new HttpClient();
    httpClient.start();
  }

  @Override
  public void tearDown() throws Exception {
    if (httpClient != null) {
      httpClient.stop();
    }
    try {
      server.stop();
    } catch (Exception ex) {
    }
    super.tearDown();
  }

  public void testAdminUI() throws Exception {
    // Not an extensive test, but it does connect to Solr and verify the Admin ui shows up.
    String adminPath = "http://127.0.0.1:" + port + "/solr/";
    try (InputStream is = URI.create(adminPath).toURL().openStream()) {
      assertNotNull(is.readAllBytes()); // real error will be an exception
    }

    var response = httpClient.GET(adminPath);
    assertEquals(200, response.getStatus());
    String header = response.getHeaders().get("X-Frame-Options");
    assertEquals("DENY", header.toUpperCase(Locale.ROOT));
  }
}

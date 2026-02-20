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
package org.apache.solr.handler.admin.api;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.solr.client.api.model.NodeSystemResponse;
import org.apache.solr.client.api.util.Constants;
import org.apache.solr.client.api.util.SolrVersion;
import org.apache.solr.client.solrj.response.XMLResponseParser;
import org.apache.solr.client.solrj.response.json.JacksonDataBindResponseParser;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SSLTestConfig;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpFields.Mutable;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test {@link GetNodeSystemInfo}. */
public class GetNodeSystemInfoTest extends SolrCloudTestCase {

  private static final int NUM_NODES = 2;

  private static final int TIMEOUT = 15000;

  private static MiniSolrCloudCluster cluster;
  private static HttpClient jettyHttpClient;

  private static JettySolrRunner jettyRunner;
  private static URL baseV2Url;
  private static String systemInfoV2Url;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cluster = configureCluster(NUM_NODES).configure();
    cluster.waitForAllNodes(TIMEOUT / 1000);
    jettyRunner = cluster.getRandomJetty(random());
    baseV2Url = jettyRunner.getBaseURLV2();
    systemInfoV2Url = baseV2Url.toString().concat(Constants.NODE_INFO_SYSTEM_PATH);

    // useSsl = true, clientAuth = false
    SSLTestConfig sslConfig = new SSLTestConfig(true, false);
    // trustAll = true
    SslContextFactory.Client factory = new SslContextFactory.Client(true);
    try {
      factory.setSslContext(sslConfig.buildClientSSLContext());
    } catch (KeyManagementException
        | UnrecoverableKeyException
        | NoSuchAlgorithmException
        | KeyStoreException e) {
      throw new IllegalStateException(
          "Unable to setup https scheme for HTTPClient to test SSL.", e);
    }

    jettyHttpClient = new HttpClient();
    jettyHttpClient.setConnectTimeout(TIMEOUT);
    jettyHttpClient.setSslContextFactory(factory);
    jettyHttpClient.setMaxConnectionsPerDestination(1);
    jettyHttpClient.setMaxRequestsQueuedPerDestination(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    jettyHttpClient.destroy();
    cluster.shutdown();
  }

  @Before
  public void beforeTest() throws Exception {
    // stop and start Jetty client for each test, otherwise, it seems responses get mixed!
    jettyHttpClient.start();
  }

  @After
  public void afterTest() throws Exception {
    jettyHttpClient.stop();
  }

  /** test through HTTP request: default Json */
  @Test
  public void testGetNodeInfoHttpJson() throws Exception {
    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(systemInfoV2Url)
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .method(HttpMethod.GET)
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("application/json", response.getMediaType());

    NodeSystemResponse infoResponse;
    JacksonDataBindResponseParser<NodeSystemResponse> parser =
        new JacksonDataBindResponseParser<>(NodeSystemResponse.class);
    try (InputStream in = new ByteArrayInputStream(response.getContent())) {
      infoResponse = parser.processToType(in, StandardCharsets.UTF_8.toString());
    }
    Assert.assertEquals(0, infoResponse.responseHeader.status);

    String expectedNode = baseV2Url.getHost() + ":" + baseV2Url.getPort() + "_solr";
    Assert.assertNotNull(infoResponse.nodeInfo);
    Assert.assertEquals(expectedNode, infoResponse.nodeInfo.node);
    // other validations in NodeSystemInfoProviderTest
  }

  /** test through HTTP request: Xml */
  @SuppressWarnings("unchecked")
  @Test
  public void testGetNodeInfoHttpXml() throws Exception {
    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(systemInfoV2Url)
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .method(HttpMethod.GET)
              .headers(
                  new Consumer<Mutable>() {

                    @Override
                    public void accept(Mutable arg0) {
                      arg0.add(HttpHeader.ACCEPT, "application/xml");
                    }
                  })
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("application/xml", response.getMediaType());

    NamedList<Object> nlResponse;
    XMLResponseParser parser = new XMLResponseParser();
    try (InputStream in = new ByteArrayInputStream(response.getContent())) {
      nlResponse = parser.processResponse(in, StandardCharsets.UTF_8.toString());
    }

    String expectedNode = baseV2Url.getHost() + ":" + baseV2Url.getPort() + "_solr";
    Assert.assertNotNull(nlResponse.get("nodeInfo"));
    Assert.assertEquals(
        expectedNode, (String) ((NamedList<Object>) nlResponse.get("nodeInfo")).get("node"));
    // other validations in NodeSystemInfoProviderTest
  }

  /** test GetNodeSystemInfo directly */
  @Test
  public void testGetNodeInfo() throws Exception {
    // Getting System info should not require a SolrCore: null
    SolrQueryRequest req = new SolrQueryRequestBase(null, new ModifiableSolrParams()) {};
    SolrQueryResponse resp = new SolrQueryResponse();

    GetNodeSystemInfo getter = new GetNodeSystemInfo(req, resp);

    NodeSystemResponse response = getter.getNodeSystemInfo(null);
    Assert.assertNotNull(response.nodeInfo);
    Assert.assertEquals("std", response.nodeInfo.mode);
    // Standalone mode : no "node"
    Assert.assertNull(response.nodeInfo.node);
    // other validations in NodeSystemInfoProviderTest
  }

  /** Test getting specific information */
  @Test
  public void testGetSpecificNodeSystemInfo() throws Exception {
    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(systemInfoV2Url.concat("/lucene"))
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .method(HttpMethod.GET)
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("application/json", response.getMediaType());

    NodeSystemResponse infoResponse;
    JacksonDataBindResponseParser<NodeSystemResponse> parser =
        new JacksonDataBindResponseParser<>(NodeSystemResponse.class);
    try (InputStream in = new ByteArrayInputStream(response.getContent())) {
      infoResponse = parser.processToType(in, StandardCharsets.UTF_8.toString());
    }
    Assert.assertNotNull(infoResponse.nodeInfo);
    Assert.assertNotNull(infoResponse.nodeInfo.lucene);
    Assert.assertEquals(
        0,
        SolrVersion.compareVersions(
            infoResponse.nodeInfo.lucene.solrSpecVersion, SolrVersion.LATEST_STRING));
    Assert.assertNull(infoResponse.nodeInfo.gpu);
    Assert.assertNull(infoResponse.nodeInfo.jvm);
    Assert.assertNull(infoResponse.nodeInfo.security);
    Assert.assertNull(infoResponse.nodeInfo.system);
  }
}

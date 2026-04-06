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
package org.apache.solr.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.cloud.SolrCloudAuthTestCase.NOT_NULL_PREDICATE;
import static org.apache.solr.security.BasicAuthIntegrationTest.STD_CONF;
import static org.apache.solr.security.BasicAuthIntegrationTest.verifySecurityStatus;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocalForTesting;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.StringRequestContent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthStandaloneTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  SecurityConfHandlerLocalForTesting securityConfHandler;
  SolrInstance instance = null;
  JettySolrRunner jetty;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    instance = new SolrInstance("inst", null);
    instance.setUp();
    jetty = createAndStartJetty(instance);
    securityConfHandler = new SecurityConfHandlerLocalForTesting(jetty.getCoreContainer());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (null != jetty) {
      jetty.stop();
      jetty = null;
    }
    super.tearDown();
  }

  @Test
  public void testBasicAuth() throws Exception {

    String authcPrefix = "/admin/authentication";
    String authzPrefix = "/admin/authorization";

    HttpClient httpClient;
    SolrClient solrClient = null;
    try {
      httpClient = jetty.getSolrClient().getHttpClient();
      String baseUrl = buildUrl(jetty.getLocalPort());
      solrClient = getHttpSolrClient(baseUrl);

      verifySecurityStatus(httpClient, baseUrl + authcPrefix, "/errorMessages", null, 20);

      // Write security.json locally. Should cause security to be initialized
      securityConfHandler.persistConf(
          new SecurityConfHandler.SecurityConfig()
              .setData(Utils.fromJSONString(STD_CONF.replace("'", "\""))));
      securityConfHandler.securityConfEdited();
      verifySecurityStatus(
          httpClient, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 20);

      String command = "{\n" + "'set-user': {'harry':'HarryIsCool'}\n" + "}";

      doHttpPost(httpClient, baseUrl + authcPrefix, command, null, null, 401);
      verifySecurityStatus(httpClient, baseUrl + authcPrefix, "authentication.enabled", "true", 20);

      command = "{\n" + "'set-user': {'harry':'HarryIsUberCool'}\n" + "}";

      doHttpPost(httpClient, baseUrl + authcPrefix, command, "solr", "SolrRocks");
      verifySecurityStatus(
          httpClient,
          baseUrl + authcPrefix,
          "authentication/credentials/harry",
          NOT_NULL_PREDICATE,
          20);

      // Read file from SOLR_HOME and verify that it contains our new user
      assertTrue(
          new String(Utils.toJSON(securityConfHandler.getSecurityConfig(false).getData()), UTF_8)
              .contains("harry"));

      // Edit authorization
      verifySecurityStatus(
          httpClient, baseUrl + authzPrefix, "authorization/permissions[1]/role", null, 20);
      doHttpPost(
          httpClient,
          baseUrl + authzPrefix,
          "{'set-permission': {'name': 'update', 'role':'updaterole'}}",
          "solr",
          "SolrRocks");
      command = "{\n" + "'set-permission': {'name': 'read', 'role':'solr'}\n" + "}";
      doHttpPost(httpClient, baseUrl + authzPrefix, command, "solr", "SolrRocks");
      try {
        solrClient.query("collection1", new MapSolrParams(Collections.singletonMap("q", "foo")));
        fail("Should return a 401 response");
      } catch (Exception e) {
        // Test that the second doPost request to /security/authorization went through
        verifySecurityStatus(
            httpClient, baseUrl + authzPrefix, "authorization/permissions[2]/role", "solr", 20);
      }
    } finally {
      if (solrClient != null) {
        solrClient.close();
      }
    }
  }

  static void doHttpPost(
      HttpClient cl, String url, String jsonCommand, String basicUser, String basicPass)
      throws IOException {
    doHttpPost(cl, url, jsonCommand, basicUser, basicPass, 200);
  }

  static void doHttpPost(
      HttpClient httpClient,
      String url,
      String jsonCommand,
      String basicUser,
      String basicPass,
      int expectStatusCode)
      throws IOException {
    doHttpPostWithHeader(
        httpClient,
        url,
        jsonCommand,
        "Authorization",
        encodeBasicAuthHeaderIfNotNull(basicUser, basicPass),
        expectStatusCode);
  }

  static void doHttpPostWithHeader(
      HttpClient httpClient,
      String url,
      String jsonCommand,
      String headerName,
      String headerValue,
      int expectStatusCode)
      throws IOException {
    try {
      var rsp =
          httpClient
              .POST(url)
              .headers(h -> h.add(headerName, headerValue))
              .body(
                  new StringRequestContent(
                      "application/json", jsonCommand.replace("'", "\""), UTF_8))
              .send();
      int statusCode = rsp.getStatus();
      assertEquals("proper_cred sent, but access denied", expectStatusCode, statusCode);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static String encodeBasicAuthHeaderIfNotNull(String user, String pwd) {
    if (user == null && pwd == null) {
      return null;
    }
    String userPass = user + ":" + pwd;
    return "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes(UTF_8));
  }

  static JettySolrRunner createAndStartJetty(SolrInstance instance) throws Exception {
    var jetty = new JettySolrRunner(instance.getHomeDir().toString(), 0);
    jetty.start();
    return jetty;
  }

  static class SolrInstance {
    String name;
    Integer port;
    Path homeDir;
    Path dataDir;

    /**
     * if leaderPort is null, this instance is a leader -- otherwise this instance is a follower,
     * and assumes the leader is on localhost at the specified port.
     */
    public SolrInstance(String name, Integer port) {
      this.name = name;
      this.port = port;
    }

    public Path getHomeDir() {
      return homeDir;
    }

    public Path getDataDir() {
      return dataDir;
    }

    public void setUp() throws Exception {
      homeDir = createTempDir(name).toAbsolutePath();
      dataDir = homeDir.resolve("collection1").resolve("data");
    }
  }
}

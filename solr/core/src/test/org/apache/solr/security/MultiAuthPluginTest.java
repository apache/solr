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

import static org.apache.solr.cloud.SolrCloudAuthTestCase.NOT_NULL_PREDICATE;
import static org.apache.solr.security.BasicAuthIntegrationTest.verifySecurityStatus;
import static org.apache.solr.security.BasicAuthStandaloneTest.SolrInstance;
import static org.apache.solr.security.BasicAuthStandaloneTest.createAndStartJetty;
import static org.apache.solr.security.BasicAuthStandaloneTest.doHttpPost;
import static org.apache.solr.security.BasicAuthStandaloneTest.doHttpPostWithHeader;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Principal;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHeader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.apache.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocalForTesting;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultiAuthPluginTest extends SolrTestCaseJ4 {

  private static final String authcPrefix = "/admin/authentication";
  private static final String authzPrefix = "/admin/authorization";

  final Predicate<Object> NULL_PREDICATE = Objects::isNull;
  SecurityConfHandlerLocalForTesting securityConfHandler;
  JettySolrRunner jetty;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    SolrInstance instance = new SolrInstance("inst", null);
    instance.setUp();
    jetty = createAndStartJetty(instance);
    securityConfHandler = new SecurityConfHandlerLocalForTesting(jetty.getCoreContainer());
    HttpClientUtil.clearRequestInterceptors(); // Clear out any old Authorization headers
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    super.tearDown();
  }

  @Test
  public void testMultiAuthEditAPI() throws Exception {
    final String user = "admin";
    final String pass = "SolrRocks";

    HttpClient httpClient = null;
    SolrClient solrClient = null;
    try {
      httpClient = HttpClientUtil.createClient(null);
      String baseUrl = buildUrl(jetty.getLocalPort());
      solrClient = getHttpSolrClient(baseUrl);

      verifySecurityStatus(httpClient, baseUrl + authcPrefix, "/errorMessages", null, 5);

      // Initialize security.json with multiple auth plugins configured
      String multiAuthPluginSecurityJson =
          Files.readString(
              TEST_PATH().resolve("security").resolve("multi_auth_plugin_security.json"),
              StandardCharsets.UTF_8);
      securityConfHandler.persistConf(
          new SecurityConfHandler.SecurityConfig()
              .setData(Utils.fromJSONString(multiAuthPluginSecurityJson)));
      securityConfHandler.securityConfEdited();

      // verify "WWW-Authenticate" headers are returned
      verifyWWWAuthenticateHeaders(httpClient, baseUrl);

      verifySecurityStatus(
          httpClient,
          baseUrl + authcPrefix,
          "authentication/class",
          "solr.MultiAuthPlugin",
          5,
          user,
          pass);
      verifySecurityStatus(
          httpClient,
          baseUrl + authzPrefix,
          "authorization/class",
          "solr.MultiAuthRuleBasedAuthorizationPlugin",
          5,
          user,
          pass);

      // anonymous requests are blocked by all plugins
      int statusCode = doHttpGetAnonymous(httpClient, baseUrl + "/admin/info/system");
      assertEquals("anonymous get succeeded but should not have", 401, statusCode);
      // update blockUnknown to allow anonymous for the basic plugin
      String command = "{\n" + "'set-property': { 'basic': {'blockUnknown':false} }\n" + "}";
      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 200);
      statusCode = doHttpGetAnonymous(httpClient, baseUrl + "/admin/info/system");
      assertEquals("anonymous get failed but should have succeeded", 200, statusCode);

      // For the multi-auth plugin, every command is wrapped with an object that identifies the
      // "scheme"
      command = "{\n" + "'set-user': {'harry':'HarryIsCool'}\n" + "}";
      // no scheme identified!
      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 400);

      command = "{\n" + "'set-user': { 'foo': {'harry':'HarryIsCool'} }\n" + "}";
      // no "foo" scheme configured
      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 400);

      command = "{\n" + "'set-user': { 'basic': {'harry':'HarryIsCool'} }\n" + "}";

      // no creds, should fail ...
      doHttpPost(httpClient, baseUrl + authcPrefix, command, null, null, 401);
      // with basic creds, should pass ...
      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 200);
      verifySecurityStatus(
          httpClient,
          baseUrl + authcPrefix,
          "authentication/schemes[0]/credentials/harry",
          NOT_NULL_PREDICATE,
          5,
          user,
          pass);

      // authz command but missing the "scheme" wrapper
      command = "{\n" + "'set-user-role': {'harry':['users']}\n" + "}";
      doHttpPost(httpClient, baseUrl + authzPrefix, command, user, pass, 400);

      // add "harry" to the "users" role ...
      command = "{\n" + "'set-user-role': { 'basic': {'harry':['users']} }\n" + "}";
      doHttpPost(httpClient, baseUrl + authzPrefix, command, user, pass, 200);
      verifySecurityStatus(
          httpClient,
          baseUrl + authzPrefix,
          "authorization/schemes[0]/user-role/harry",
          NOT_NULL_PREDICATE,
          5,
          user,
          pass);

      // give the users role a custom permission
      verifySecurityStatus(
          httpClient,
          baseUrl + authzPrefix,
          "authorization/permissions[6]",
          NULL_PREDICATE,
          5,
          user,
          pass);
      command =
          "{\n"
              + "'set-permission': { 'name':'k8s-zk', 'role':'users', 'collection':null, 'path':'/admin/zookeeper/status' }\n"
              + "}";
      doHttpPost(httpClient, baseUrl + authzPrefix, command, user, pass, 200);
      verifySecurityStatus(
          httpClient,
          baseUrl + authzPrefix,
          "authorization/permissions[6]/path",
          new ExpectedValuePredicate("/admin/zookeeper/status"),
          5,
          user,
          pass);

      command =
          "{\n"
              + "'update-permission': { 'index':'7', 'name':'k8s-zk', 'role':'users', 'collection':null, 'path':'/admin/zookeeper/status2' }\n"
              + "}";
      doHttpPost(httpClient, baseUrl + authzPrefix, command, user, pass, 200);
      verifySecurityStatus(
          httpClient,
          baseUrl + authzPrefix,
          "authorization/permissions[6]/path",
          new ExpectedValuePredicate("/admin/zookeeper/status2"),
          5,
          user,
          pass);

      // delete the permission
      command = "{\n" + "'delete-permission': 7\n" + "}";
      doHttpPost(httpClient, baseUrl + authzPrefix, command, user, pass, 200);
      verifySecurityStatus(
          httpClient,
          baseUrl + authzPrefix,
          "authorization/permissions[6]",
          NULL_PREDICATE,
          5,
          user,
          pass);

      // delete the user
      command = "{\n" + "'delete-user': { 'basic': 'harry' }\n" + "}";

      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 200);
      verifySecurityStatus(
          httpClient,
          baseUrl + authcPrefix,
          "authentication/schemes[0]/credentials/harry",
          NULL_PREDICATE,
          5,
          user,
          pass);

      // update the property on the mock (just to test routing to the mock plugin)
      command = "{\n" + "'set-property': { 'mock': { 'blockUnknown':false } }\n" + "}";

      doHttpPostWithHeader(
          httpClient,
          baseUrl + authcPrefix,
          command,
          new BasicHeader("Authorization", "mock foo"),
          200);
      verifySecurityStatus(
          httpClient,
          baseUrl + authcPrefix,
          "authentication/schemes[1]/blockUnknown",
          new ExpectedValuePredicate(Boolean.FALSE),
          5,
          user,
          pass);
    } finally {
      if (httpClient != null) {
        HttpClientUtil.close(httpClient);
      }
      if (solrClient != null) {
        solrClient.close();
      }
    }
  }

  @Test
  public void testMultiAuthXBasicLookup() throws Exception {
    final String user = "admin";
    final String pass = "SolrRocks";

    HttpClient httpClient = null;
    SolrClient solrClient = null;
    try {
      httpClient = HttpClientUtil.createClient(null);
      String baseUrl = buildUrl(jetty.getLocalPort());
      solrClient = getHttpSolrClient(baseUrl);

      verifySecurityStatus(httpClient, baseUrl + authcPrefix, "/errorMessages", null, 5);

      // Initialize security.json with multiple xbasic auth and other configured
      String multiAuthPluginSecurityJson =
          Files.readString(
              TEST_PATH()
                  .resolve("security")
                  .resolve("multi_auth_plugin_with_xbasic_security.json"),
              StandardCharsets.UTF_8);
      securityConfHandler.persistConf(
          new SecurityConfHandler.SecurityConfig()
              .setData(Utils.fromJSONString(multiAuthPluginSecurityJson)));
      securityConfHandler.securityConfEdited();

      // verify "WWW-Authenticate" headers are returned
      verifyWWWAuthenticateHeaders(httpClient, baseUrl);

      // Command that does not update anything in the current config
      String command = "{ 'set-property': { 'xbasic': { 'blockUnknown': true } } }";

      // verify that clients can still use "Basic" scheme with xBasic scheme configured in MultiAuth
      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 200);
    } finally {
      if (httpClient != null) {
        HttpClientUtil.close(httpClient);
      }
      if (solrClient != null) {
        solrClient.close();
      }
    }
  }

  @Test
  public void testMultiAuthWithBasicAndXBasic() throws Exception {
    final String user = "admin";
    final String xUser = "xadmin";
    final String pass = "SolrRocks";

    HttpClient httpClient = null;
    SolrClient solrClient = null;
    try {
      httpClient = HttpClientUtil.createClient(null);
      String baseUrl = buildUrl(jetty.getLocalPort());
      solrClient = getHttpSolrClient(baseUrl);

      verifySecurityStatus(httpClient, baseUrl + authcPrefix, "/errorMessages", null, 5);

      // Initialize security.json with basic and xbasic scheme
      String multiAuthPluginSecurityJson =
          Files.readString(
              TEST_PATH()
                  .resolve("security")
                  .resolve("multi_auth_plugin_with_basic_and_xbasic_security.json"),
              StandardCharsets.UTF_8);
      securityConfHandler.persistConf(
          new SecurityConfHandler.SecurityConfig()
              .setData(Utils.fromJSONString(multiAuthPluginSecurityJson)));
      securityConfHandler.securityConfEdited();

      // verify "WWW-Authenticate" headers are returned
      verifyWWWAuthenticateHeaders(httpClient, baseUrl);

      // Command that does not update anything in the current config
      String command = "{ 'set-property': { 'basic': { 'blockUnknown': true } } }";

      // verify that basic takes precedence over xbasic when both present
      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 200);

      // Since both are present, xbasic will never be looked up if client does not send XBasic
      // as auth scheme, and using xBasic won't work with BasicAuthPlugin, so this security
      // configuration should return 401 as it resolves with the plugin that uses "basic" as scheme
      doHttpPost(httpClient, baseUrl + authcPrefix, command, xUser, pass, 401);
    } finally {
      if (httpClient != null) {
        HttpClientUtil.close(httpClient);
      }
      if (solrClient != null) {
        solrClient.close();
      }
    }
  }

  @Test
  public void testMultiAuthWithSinglePlugin() throws Exception {
    final String user = "admin";
    final String pass = "SolrRocks";

    HttpClient httpClient = null;
    SolrClient solrClient = null;
    try {
      httpClient = HttpClientUtil.createClient(null);
      String baseUrl = buildUrl(jetty.getLocalPort());
      solrClient = getHttpSolrClient(baseUrl);

      verifySecurityStatus(httpClient, baseUrl + authcPrefix, "/errorMessages", null, 5);

      // Initialize security.json with a single plugin configured
      String multiAuthPluginSecurityJson =
          Files.readString(
              TEST_PATH()
                  .resolve("security")
                  .resolve("multi_auth_plugin_with_basic_only_security.json"),
              StandardCharsets.UTF_8);
      securityConfHandler.persistConf(
          new SecurityConfHandler.SecurityConfig()
              .setData(Utils.fromJSONString(multiAuthPluginSecurityJson)));
      securityConfHandler.securityConfEdited();

      // verify "WWW-Authenticate" headers are returned
      verifyWWWAuthenticateHeaders(httpClient, baseUrl);

      // Command that does not update anything in the current config
      String command = "{ 'set-property': { 'basic': { 'blockUnknown': true } } }";

      // verify that a single plugin configuration is allowed and works
      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 200);
    } finally {
      if (httpClient != null) {
        HttpClientUtil.close(httpClient);
      }
      if (solrClient != null) {
        solrClient.close();
      }
    }
  }

  @Test
  public void testMultiAuthWithBasicAndMockPlugin() throws Exception {
    final String user = "admin";
    final String pass = "SolrRocks";

    HttpClient httpClient = null;
    SolrClient solrClient = null;
    try {
      httpClient = HttpClientUtil.createClient(null);
      String baseUrl = buildUrl(jetty.getLocalPort());
      solrClient = getHttpSolrClient(baseUrl);

      verifySecurityStatus(httpClient, baseUrl + authcPrefix, "/errorMessages", null, 5);

      // Initialize security.json with a single plugin configured
      String multiAuthPluginSecurityJson =
          Files.readString(
              TEST_PATH()
                  .resolve("security")
                  .resolve("multi_auth_plugin_with_mock_and_basic_security.json"),
              StandardCharsets.UTF_8);
      securityConfHandler.persistConf(
          new SecurityConfHandler.SecurityConfig()
              .setData(Utils.fromJSONString(multiAuthPluginSecurityJson)));
      securityConfHandler.securityConfEdited();

      // verify "WWW-Authenticate" headers are returned
      verifyWWWAuthenticateHeaders(httpClient, baseUrl);

      // Command that does not update anything in the current config
      String command = "{ 'set-property': { 'basic': { 'blockUnknown': true } } }";

      // verify that the basic auth plugin works and is looked up as expected
      doHttpPost(httpClient, baseUrl + authcPrefix, command, user, pass, 200);
    } finally {
      if (httpClient != null) {
        HttpClientUtil.close(httpClient);
      }
      if (solrClient != null) {
        solrClient.close();
      }
    }
  }

  @Test
  public void testMultiAuthWithBasicPluginAndAjax() throws Exception {
    HttpClient httpClient = null;
    SolrClient solrClient = null;
    try {
      httpClient = HttpClientUtil.createClient(null);
      String baseUrl = buildUrl(jetty.getLocalPort());
      solrClient = getHttpSolrClient(baseUrl);

      verifySecurityStatus(httpClient, baseUrl + authcPrefix, "/errorMessages", null, 5);

      // Initialize security.json with a single plugin configured
      String multiAuthPluginSecurityJson =
          Files.readString(
              TEST_PATH().resolve("security").resolve("multi_auth_plugin_security.json"),
              StandardCharsets.UTF_8);
      securityConfHandler.persistConf(
          new SecurityConfHandler.SecurityConfig()
              .setData(Utils.fromJSONString(multiAuthPluginSecurityJson)));
      securityConfHandler.securityConfEdited();

      // Pretend to send unauthorized AJAX request
      HttpGet httpGet = new HttpGet(baseUrl + "/admin/info/system");
      httpGet.addHeader(new BasicHeader("X-Requested-With", "XMLHttpRequest"));

      HttpResponse response = httpClient.execute(httpGet);
      assertEquals(
          "Unauthorized response was expected", 401, response.getStatusLine().getStatusCode());

      // Only first plugin is expected as response, which is also xBasic if BasicAuthPlugin
      Header[] headers = response.getHeaders(HttpHeaders.WWW_AUTHENTICATE);
      List<String> actualSchemes = Arrays.stream(headers).map(Header::getValue).toList();

      // Only the first scheme is expected for AJAX-Requests
      assertEquals("Only one scheme was expected", 1, actualSchemes.size());

      // In case of BasicAuthPlugin, xBasic should be returned if AJAX request sent and handled by
      // BasicAuthPlugin
      String expectedScheme = "xBasic realm=\"solr\"";
      assertEquals(
          "Mapped xBasic challenge expected from first plugin which is BasicAuthPlugin",
          expectedScheme,
          actualSchemes.getFirst());
    } finally {
      if (httpClient != null) {
        HttpClientUtil.close(httpClient);
      }
      if (solrClient != null) {
        solrClient.close();
      }
    }
  }

  private int doHttpGetAnonymous(HttpClient cl, String url) throws IOException {
    HttpGet httpPost = new HttpGet(url);
    HttpResponse r = cl.execute(httpPost);
    int statusCode = r.getStatusLine().getStatusCode();
    HttpClientUtil.consumeFully(r.getEntity());
    return statusCode;
  }

  private void verifyWWWAuthenticateHeaders(HttpClient httpClient, String baseUrl)
      throws Exception {
    HttpGet httpGet = new HttpGet(baseUrl + "/admin/info/system");
    HttpResponse response = httpClient.execute(httpGet);
    Header[] headers = response.getHeaders(HttpHeaders.WWW_AUTHENTICATE);
    List<String> actualSchemes =
        Arrays.stream(headers).map(Header::getValue).collect(Collectors.toList());

    List<String> expectedSchemes = generateExpectedSchemes();
    actualSchemes.sort(String.CASE_INSENSITIVE_ORDER);
    expectedSchemes.sort(String.CASE_INSENSITIVE_ORDER);

    assertEquals(
        "The actual schemes and realms should match the expected ones exactly",
        expectedSchemes.stream().map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toList()),
        actualSchemes.stream().map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toList()));
  }

  @SuppressWarnings("unchecked")
  private List<String> generateExpectedSchemes() {
    Map<String, Object> data = securityConfHandler.getSecurityConfig(false).getData();
    Map<String, Object> authentication = (Map<String, Object>) data.get("authentication");
    List<Map<String, Object>> schemes = (List<Map<String, Object>>) authentication.get("schemes");

    return schemes.stream()
        .map(
            schemeMap -> {
              String scheme = (String) schemeMap.get("scheme");
              String realm = (String) schemeMap.get("realm");
              return realm != null ? scheme + " realm=\"" + realm + "\"" : scheme;
            })
        .collect(Collectors.toList());
  }

  private static final class MockPrincipal implements Principal, Serializable {
    @Override
    public String getName() {
      return "mock";
    }
  }

  public static final class MockAuthPluginForTesting extends AuthenticationPlugin
      implements ConfigEditablePlugin {

    @Override
    public void init(Map<String, Object> pluginConfig) {}

    @Override
    public boolean doAuthenticate(
        HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
        throws Exception {
      Principal principal = new MockPrincipal();
      request = wrapWithPrincipal(request, principal, "mock");
      filterChain.doFilter(request, response);
      return true;
    }

    @Override
    public Map<String, Object> edit(
        Map<String, Object> latestConf, List<CommandOperation> commands) {
      for (CommandOperation op : commands) {
        if ("set-property".equals(op.name)) {
          for (Map.Entry<String, Object> e : op.getDataMap().entrySet()) {
            if ("blockUnknown".equals(e.getKey())) {
              latestConf.put(e.getKey(), e.getValue());
              return latestConf;
            } else {
              op.addError("Unknown property " + e.getKey());
            }
          }
        } else {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Unsupported command: " + op.name);
        }
      }
      return null;
    }
  }

  private static final class ExpectedValuePredicate implements Predicate<Object> {
    final Object expected;

    ExpectedValuePredicate(Object exp) {
      this.expected = exp;
    }

    @Override
    public boolean test(Object s) {
      return expected.equals(s);
    }
  }
}

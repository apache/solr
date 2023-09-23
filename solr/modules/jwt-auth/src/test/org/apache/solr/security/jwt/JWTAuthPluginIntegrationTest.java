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
package org.apache.solr.security.jwt;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.security.jwt.JWTAuthPluginTest.JWT_TEST_PATH;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.OAuth2Config;
import no.nav.security.mock.oauth2.http.MockWebServerWrapper;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.CryptoKeys;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.TimeOut;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.lang.JoseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Validate that JWT token authentication works in a real cluster.
 *
 * <p>TODO: Test also using SolrJ as client. But that requires a way to set Authorization header on
 * request, see SOLR-13070<br>
 * This is also the reason we use {@link org.apache.solr.SolrTestCaseJ4.SuppressSSL} annotation,
 * since we use HttpUrlConnection
 */
@SolrTestCaseJ4.SuppressSSL
public class JWTAuthPluginIntegrationTest extends SolrCloudAuthTestCase {

  private static String mockOAuthToken;
  private static Path pemFilePath;
  private static Path wrongPemFilePath;
  private static String jwtStaticTestToken;
  private static JsonWebSignature jws;
  private static String jwtTokenWrongSignature;
  private static MockOAuth2Server mockOAuth2Server;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Setup an OAuth2 mock server with SSL
    Path p12Cert = JWT_TEST_PATH().resolve("security").resolve("jwt_plugin_idp_certs.p12");
    pemFilePath = JWT_TEST_PATH().resolve("security").resolve("jwt_plugin_idp_cert.pem");
    wrongPemFilePath = JWT_TEST_PATH().resolve("security").resolve("jwt_plugin_idp_wrongcert.pem");

    mockOAuth2Server = createMockOAuthServer(p12Cert, "secret");
    mockOAuth2Server.start();
    mockOAuthToken =
        mockOAuth2Server
            .issueToken("default", "myClientId", new DefaultOAuth2TokenCallback())
            .serialize();
    initStaticJwt();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (mockOAuth2Server != null) {
      mockOAuth2Server.shutdown();
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    shutdownCluster();
    super.tearDown();
  }

  @Test
  @BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-15484")
  public void mockOAuth2Server() throws Exception {
    MiniSolrCloudCluster myCluster = configureClusterMockOauth(2, pemFilePath, 10000);
    String baseUrl = myCluster.getRandomJetty(random()).getBaseUrl().toString();

    // First attempt without token fails
    Map<String, String> headers = getHeaders(baseUrl + "/admin/info/system", null);
    assertEquals("Should have received 401 code", "401", headers.get("code"));

    // Second attempt with token from Oauth mock server succeeds
    headers = getHeaders(baseUrl + "/admin/info/system", mockOAuthToken);
    assertEquals("200", headers.get("code"));
    myCluster.shutdown();
  }

  @Test
  public void mockOAuth2ServerWrongPEMInTruststore() {
    // JWTAuthPlugin throws SSLHandshakeException when fetching JWK, so this trips cluster init
    assertThrows(Exception.class, () -> configureClusterMockOauth(2, wrongPemFilePath, 2000));
  }

  public void testStaticJwtKeys() throws Exception {
    MiniSolrCloudCluster myCluster = configureClusterStaticKeys("jwt_plugin_jwk_security.json");
    String baseUrl = myCluster.getRandomJetty(random()).getBaseUrl().toString();

    // No token fails
    assertThrows(IOException.class, () -> get(baseUrl + "/admin/info/system", null));

    // Validate X-Solr-AuthData headers
    Map<String, String> headers = getHeaders(baseUrl + "/admin/info/system", null);
    assertEquals("Should have received 401 code", "401", headers.get("code"));
    assertEquals("Bearer realm=\"my-solr-jwt\"", headers.get("WWW-Authenticate"));
    String authData = new String(Base64.getDecoder().decode(headers.get("X-Solr-AuthData")), UTF_8);
    assertEquals(
        "{\n"
            + "  \"tokenEndpoint\":\"http://acmepaymentscorp/oauth/oauth20/token\",\n"
            + "  \"authorization_flow\":\"code_pkce\",\n"
            + "  \"scope\":\"solr:admin\",\n"
            + "  \"redirect_uris\":[],\n"
            + "  \"authorizationEndpoint\":\"http://acmepaymentscorp/oauth/auz/authorize\",\n"
            + "  \"client_id\":\"solr-cluster\"}",
        authData);
    myCluster.shutdown();
  }

  @Test
  public void infoRequestValidateXSolrAuthHeadersBlockUnknownFalse() throws Exception {
    // https://issues.apache.org/jira/browse/SOLR-14196
    MiniSolrCloudCluster myCluster =
        configureClusterStaticKeys("jwt_plugin_jwk_security_blockUnknownFalse.json");
    String baseUrl = myCluster.getRandomJetty(random()).getBaseUrl().toString();

    Map<String, String> headers = getHeaders(baseUrl + "/admin/info/system", null);
    assertEquals("Should have received 401 code", "401", headers.get("code"));
    assertEquals(
        "Bearer realm=\"my-solr-jwt-blockunknown-false\"", headers.get("WWW-Authenticate"));
    String authData = new String(Base64.getDecoder().decode(headers.get("X-Solr-AuthData")), UTF_8);
    assertEquals(
        "{\n"
            + "  \"tokenEndpoint\":\"http://acmepaymentscorp/oauth/oauth20/token\",\n"
            + "  \"authorization_flow\":\"code_pkce\",\n"
            + "  \"scope\":\"solr:admin\",\n"
            + "  \"redirect_uris\":[],\n"
            + "  \"authorizationEndpoint\":\"http://acmepaymentscorp/oauth/auz/authorize\",\n"
            + "  \"client_id\":\"solr-cluster\"}",
        authData);
    myCluster.shutdown();
  }

  @Test
  public void testMetrics() throws Exception {
    // Here we use the "global" class-level cluster variable, but not for the other tests
    cluster = configureClusterStaticKeys("jwt_plugin_jwk_security.json");

    boolean isUseV2Api = random().nextBoolean();
    String authcPrefix = "/admin/authentication";
    if (isUseV2Api) {
      authcPrefix = "/____v2/cluster/security/authentication";
    }
    String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    CloseableHttpClient cl = HttpClientUtil.createClient(null);

    String COLLECTION = "jwtColl";
    createCollection(cluster, COLLECTION);

    // Missing token
    getAndFail(baseUrl + "/" + COLLECTION + "/query?q=*:*", null);
    assertAuthMetricsMinimums(2, 1, 0, 0, 1, 0);
    executeCommand(baseUrl + authcPrefix, cl, "{set-property : { blockUnknown: false}}", jws);
    verifySecurityStatus(
        cl,
        baseUrl + authcPrefix,
        "authentication/blockUnknown",
        "false",
        20,
        getBearerAuthHeader(jws));
    // Pass through
    verifySecurityStatus(cl, baseUrl + "/admin/info/key", "key", NOT_NULL_PREDICATE, 20);
    // Now succeeds since blockUnknown=false
    get(baseUrl + "/" + COLLECTION + "/query?q=*:*", null);
    executeCommand(baseUrl + authcPrefix, cl, "{set-property : { blockUnknown: true}}", null);
    verifySecurityStatus(
        cl,
        baseUrl + authcPrefix,
        "authentication/blockUnknown",
        "true",
        20,
        getBearerAuthHeader(jws));

    assertAuthMetricsMinimums(9, 4, 4, 0, 1, 0);

    // Wrong Credentials
    getAndFail(baseUrl + "/" + COLLECTION + "/query?q=*:*", jwtTokenWrongSignature);
    assertAuthMetricsMinimums(10, 4, 4, 1, 1, 0);

    // JWT parse error
    getAndFail(baseUrl + "/" + COLLECTION + "/query?q=*:*", "foozzz");
    assertAuthMetricsMinimums(11, 4, 4, 1, 1, 1);

    // Merged with createCollectionUpdateAndQueryDistributed()
    // Now update three documents
    assertAuthMetricsMinimums(1, 1, 0, 0, 0, 0);
    assertPkiAuthMetricsMinimums(2, 2, 0, 0, 0, 0);
    Pair<String, Integer> result =
        post(
            baseUrl + "/" + COLLECTION + "/update?commit=true",
            "[{\"id\" : \"1\"}, {\"id\": \"2\"}, {\"id\": \"3\"}]",
            jwtStaticTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    assertAuthMetricsMinimums(4, 4, 0, 0, 0, 0);
    assertPkiAuthMetricsMinimums(2, 2, 0, 0, 0, 0);

    // First a non distributed query
    result = get(baseUrl + "/" + COLLECTION + "/query?q=*:*&distrib=false", jwtStaticTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    assertAuthMetricsMinimums(5, 5, 0, 0, 0, 0);

    // Now do a distributed query, using JWTAuth for inter-node
    result = get(baseUrl + "/" + COLLECTION + "/query?q=*:*", jwtStaticTestToken);
    assertEquals(Integer.valueOf(200), result.second());
    assertAuthMetricsMinimums(10, 10, 0, 0, 0, 0);

    // Delete
    assertEquals(
        200,
        get(baseUrl + "/admin/collections?action=DELETE&name=" + COLLECTION, jwtStaticTestToken)
            .second()
            .intValue());
    assertAuthMetricsMinimums(11, 11, 0, 0, 0, 0);
    assertPkiAuthMetricsMinimums(4, 4, 0, 0, 0, 0);

    HttpClientUtil.close(cl);
  }

  static String getBearerAuthHeader(JsonWebSignature jws) throws JoseException {
    return "Bearer " + jws.getCompactSerialization();
  }

  /**
   * Configure solr cluster with a security.json talking to MockOAuth2 server
   *
   * @param numNodes number of nodes in cluster
   * @param pemFilePath path to PEM file for SSL cert to trust for OAuth2 server
   * @param timeoutMs how long to wait until the new security.json is applied to the cluster
   * @return an instance of the created cluster that the test can talk to
   */
  @SuppressWarnings("BusyWait")
  private MiniSolrCloudCluster configureClusterMockOauth(
      int numNodes, Path pemFilePath, long timeoutMs) throws Exception {
    MiniSolrCloudCluster myCluster =
        configureCluster(numNodes) // nodes
            .addConfig(
                "conf1",
                JWT_TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
            .withDefaultClusterProperty("useLegacyReplicaAssignment", "false")
            .build();
    String securityJson = createMockOAuthSecurityJson(pemFilePath);
    myCluster.zkSetData("/security.json", securityJson.getBytes(Charset.defaultCharset()), true);
    RTimer timer = new RTimer();
    do { // Wait timeoutMs time for the security.json change to take effect
      Thread.sleep(200);
      if (timer.getTime() > timeoutMs) {
        myCluster.shutdown();
        throw new Exception("Custom 'security.json' not applied in " + timeoutMs + "ms");
      }
    } while (myCluster.getJettySolrRunner(0).getCoreContainer().getAuthenticationPlugin() == null);
    myCluster.waitForAllNodes(10);
    return myCluster;
  }

  /**
   * Configure solr cluster with a security.json made for static keys
   *
   * @param securityJsonFilename file name of test json, relative to test-files/solr/security
   * @return an instance of the created cluster that the test can talk to
   */
  private MiniSolrCloudCluster configureClusterStaticKeys(String securityJsonFilename)
      throws Exception {
    MiniSolrCloudCluster myCluster =
        configureCluster(2) // nodes
            .withSecurityJson(JWT_TEST_PATH().resolve("security").resolve(securityJsonFilename))
            .addConfig(
                "conf1",
                JWT_TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
            .withDefaultClusterProperty("useLegacyReplicaAssignment", "false")
            .build();

    myCluster.waitForAllNodes(10);
    return myCluster;
  }

  /** Initialize some static JWT keys */
  private static void initStaticJwt() throws Exception {
    String jwkJSON =
        "{\n"
            + "  \"kty\": \"RSA\",\n"
            + "  \"d\": \"i6pyv2z3o-MlYytWsOr3IE1olu2RXZBzjPRBNgWAP1TlLNaphHEvH5aHhe_CtBAastgFFMuP29CFhaL3_tGczkvWJkSveZQN2AHWHgRShKgoSVMspkhOt3Ghha4CvpnZ9BnQzVHnaBnHDTTTfVgXz7P1ZNBhQY4URG61DKIF-JSSClyh1xKuMoJX0lILXDYGGcjVTZL_hci4IXPPTpOJHV51-pxuO7WU5M9252UYoiYyCJ56ai8N49aKIMsqhdGuO4aWUwsGIW4oQpjtce5eEojCprYl-9rDhTwLAFoBtjy6LvkqlR2Ae5dKZYpStljBjK8PJrBvWZjXAEMDdQ8PuQ\",\n"
            + "  \"e\": \"AQAB\",\n"
            + "  \"use\": \"sig\",\n"
            + "  \"kid\": \"test\",\n"
            + "  \"alg\": \"RS256\",\n"
            + "  \"n\": \"jeyrvOaZrmKWjyNXt0myAc_pJ1hNt3aRupExJEx1ewPaL9J9HFgSCjMrYxCB1ETO1NDyZ3nSgjZis-jHHDqBxBjRdq_t1E2rkGFaYbxAyKt220Pwgme_SFTB9MXVrFQGkKyjmQeVmOmV6zM3KK8uMdKQJ4aoKmwBcF5Zg7EZdDcKOFgpgva1Jq-FlEsaJ2xrYDYo3KnGcOHIt9_0NQeLsqZbeWYLxYni7uROFncXYV5FhSJCeR4A_rrbwlaCydGxE0ToC_9HNYibUHlkJjqyUhAgORCbNS8JLCJH8NUi5sDdIawK9GTSyvsJXZ-QHqo4cMUuxWV5AJtaRGghuMUfqQ\"\n"
            + "}";

    PublicJsonWebKey jwk = RsaJsonWebKey.Factory.newPublicJwk(jwkJSON);
    JwtClaims claims = JWTAuthPluginTest.generateClaims();
    jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(jwk.getPrivateKey());
    jws.setKeyIdHeaderValue(jwk.getKeyId());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);

    jwtStaticTestToken = jws.getCompactSerialization();

    PublicJsonWebKey jwk2 = RsaJwkGenerator.generateJwk(2048);
    jwk2.setKeyId("k2");
    JsonWebSignature jws2 = new JsonWebSignature();
    jws2.setPayload(claims.toJson());
    jws2.setKey(jwk2.getPrivateKey());
    jws2.setKeyIdHeaderValue(jwk2.getKeyId());
    jws2.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
    jwtTokenWrongSignature = jws2.getCompactSerialization();
  }

  private void getAndFail(String url, String token) {
    try {
      get(url, token);
      fail("Request to " + url + " with token " + token + " should have failed");
    } catch (Exception e) {
      /* Fall through */
    }
  }

  private Pair<String, Integer> get(String url, String token) throws IOException {
    URL createUrl = new URL(url);
    HttpURLConnection createConn = (HttpURLConnection) createUrl.openConnection();
    if (token != null) createConn.setRequestProperty("Authorization", "Bearer " + token);
    BufferedReader br2 =
        new BufferedReader(
            new InputStreamReader((InputStream) createConn.getContent(), StandardCharsets.UTF_8));
    String result = br2.lines().collect(Collectors.joining("\n"));
    int code = createConn.getResponseCode();
    createConn.disconnect();
    return new Pair<>(result, code);
  }

  private Map<String, String> getHeaders(String url, String token) throws IOException {
    URL createUrl = new URL(url);
    HttpURLConnection conn = (HttpURLConnection) createUrl.openConnection();
    if (token != null) conn.setRequestProperty("Authorization", "Bearer " + token);
    conn.connect();
    int code = conn.getResponseCode();
    Map<String, String> result = new HashMap<>();
    conn.getHeaderFields().forEach((k, v) -> result.put(k, v.get(0)));
    result.put("code", String.valueOf(code));
    conn.disconnect();
    return result;
  }

  private Pair<String, Integer> post(String url, String json, String token) throws IOException {
    URL createUrl = new URL(url);
    HttpURLConnection con = (HttpURLConnection) createUrl.openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    if (token != null) con.setRequestProperty("Authorization", "Bearer " + token);

    con.setDoOutput(true);
    OutputStream os = con.getOutputStream();
    os.write(json.getBytes(StandardCharsets.UTF_8));
    os.flush();
    os.close();

    con.connect();
    BufferedReader br2 =
        new BufferedReader(
            new InputStreamReader((InputStream) con.getContent(), StandardCharsets.UTF_8));
    String result = br2.lines().collect(Collectors.joining("\n"));
    int code = con.getResponseCode();
    con.disconnect();
    return new Pair<>(result, code);
  }

  private void createCollection(MiniSolrCloudCluster myCluster, String collectionName)
      throws IOException {
    String baseUrl = myCluster.getRandomJetty(random()).getBaseUrl().toString();
    assertEquals(
        200,
        get(
                baseUrl
                    + "/admin/collections?action=CREATE&name="
                    + collectionName
                    + "&numShards=2",
                jwtStaticTestToken)
            .second()
            .intValue());
    myCluster.waitForActiveCollection(collectionName, 2, 2);
  }

  private void executeCommand(String url, HttpClient cl, String payload, JsonWebSignature jws)
      throws Exception {

    // HACK: work around for SOLR-13464...
    //
    // note the authz/authn objects in use on each node before executing the command,
    // then wait until we see new objects on every node *after* executing the command
    // before returning...
    final Set<Map.Entry<String, Object>> initialPlugins =
        getAuthPluginsInUseForCluster(url).entrySet();

    HttpPost httpPost;
    HttpResponse r;
    httpPost = new HttpPost(url);
    if (jws != null) setAuthorizationHeader(httpPost, "Bearer " + jws.getCompactSerialization());
    httpPost.setEntity(new ByteArrayEntity(payload.getBytes(UTF_8)));
    httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
    r = cl.execute(httpPost);
    String response = new String(r.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    assertEquals(
        "Non-200 response code. Response was " + response, 200, r.getStatusLine().getStatusCode());
    assertFalse("Response contained errors: " + response, response.contains("errorMessages"));
    Utils.consumeFully(r.getEntity());

    // HACK (continued)...
    final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeout.waitFor(
        "core containers never fully updated their auth plugins",
        () -> {
          final Set<Map.Entry<String, Object>> tmpSet =
              getAuthPluginsInUseForCluster(url).entrySet();
          tmpSet.retainAll(initialPlugins);
          return tmpSet.isEmpty();
        });
  }

  /**
   * Creates a security.json string which points to the MockOAuth server using it's well-known URL
   * and trusting its SSL
   */
  private static String createMockOAuthSecurityJson(Path pemFilePath) throws IOException {
    String wellKnown = mockOAuth2Server.wellKnownUrl("default").toString();
    String pemCert =
        CryptoKeys.extractCertificateFromPem(Files.readString(pemFilePath))
            .replace("\\n", "\\\\n"); // Use literal \n to play well with JSON
    return "{\n"
        + "  \"authentication\" : {\n"
        + "    \"class\": \"solr.JWTAuthPlugin\",\n"
        + "    \"wellKnownUrl\": \""
        + wellKnown
        + "\",\n"
        + "    \"blockUnknown\": true\n"
        + "    \"trustedCerts\": \""
        + pemCert
        + "\"\n"
        + "  }\n"
        + "}";
  }

  /**
   * Create and return a MockOAuth2Server with given SSL certificate
   *
   * @param p12CertPath path to a p12 certificate store
   * @param secretKeyPass password to secret key
   */
  private static MockOAuth2Server createMockOAuthServer(Path p12CertPath, String secretKeyPass) {
    try {
      final KeyStore keystore = KeyStore.getInstance("pkcs12");
      keystore.load(Files.newInputStream(p12CertPath), secretKeyPass.toCharArray());
      final KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keystore, secretKeyPass.toCharArray());
      final TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(keystore);

      MockWebServer mockWebServer;
      try (MockWebServerWrapper mockWebServerWrapper = new MockWebServerWrapper()) {
        mockWebServer = mockWebServerWrapper.getMockWebServer();
      }
      SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(
          keyManagerFactory.getKeyManagers(), /*trustManagerFactory.getTrustManagers()*/
          null,
          null);
      SSLSocketFactory sf = sslContext.getSocketFactory();
      mockWebServer.useHttps(sf, false);

      OAuth2Config config = new OAuth2Config();
      ((MockWebServerWrapper) config.getHttpServer()).getMockWebServer().useHttps(sf, false);

      return new MockOAuth2Server(config);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed initializing SSL", e);
    }
  }
}

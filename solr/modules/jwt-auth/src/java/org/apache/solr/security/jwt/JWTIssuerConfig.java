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

import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jose.util.Resource;
import com.nimbusds.jose.util.ResourceRetriever;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds information about an IdP (issuer), such as issuer ID, JWK url(s), keys etc */
public class JWTIssuerConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final String PARAM_ISS_NAME = "name";
  static final String PARAM_JWKS_URL = "jwksUrl";
  static final String PARAM_JWK = "jwk";
  static final String PARAM_ISSUER = "iss";
  static final String PARAM_AUDIENCE = "aud";
  static final String PARAM_WELL_KNOWN_URL = "wellKnownUrl";
  static final String PARAM_AUTHORIZATION_ENDPOINT = "authorizationEndpoint";
  static final String PARAM_TOKEN_ENDPOINT = "tokenEndpoint";
  static final String PARAM_CLIENT_ID = "clientId";
  static final String PARAM_AUTHORIZATION_FLOW = "authorizationFlow";

  private static HttpsJwksFactory httpsJwksFactory = new HttpsJwksFactory(3600, 5000);
  private String iss;
  private String aud;
  private JWKSet jsonWebKeySet;
  private String name;
  private List<String> jwksUrl;
  private List<JwkSetFetcher> httpsJwks;
  private String wellKnownUrl;
  private WellKnownDiscoveryConfig wellKnownDiscoveryConfig;
  private String clientId;
  private String authorizationEndpoint;
  private String tokenEndpoint;
  private String authorizationFlow;
  private Collection<X509Certificate> trustedCerts;

  public static boolean ALLOW_OUTBOUND_HTTP =
      EnvUtils.getPropertyAsBool("solr.auth.jwt.outbound.http.enabled", false);
  public static final String ALLOW_OUTBOUND_HTTP_ERR_MSG =
      "HTTPS required for IDP communication. Please use SSL or start your nodes with -Dsolr.auth.jwt.outbound.http.enabled=true to allow HTTP for test purposes.";
  private static final String DEFAULT_AUTHORIZATION_FLOW =
      "implicit"; // 'implicit' to be deprecated
  private static final Set<String> VALID_AUTHORIZATION_FLOWS =
      Set.of(DEFAULT_AUTHORIZATION_FLOW, "code_pkce");

  /**
   * Create config for further configuration with setters, builder style. Once all values are set,
   * call {@link #init()} before further use
   *
   * @param name a unique name for this issuer
   */
  public JWTIssuerConfig(String name) {
    this.name = name;
  }

  /**
   * Initialize issuer config from a generic configuration map
   *
   * @param configMap map of configuration keys anv values
   */
  public JWTIssuerConfig(Map<String, Object> configMap) {
    parseConfigMap(configMap);
  }

  /**
   * Call this to validate and initialize an object which is populated with setters. Init will fetch
   * wellKnownUrl if relevant
   *
   * @throws SolrException if issuer is missing
   */
  public void init() {
    if (!isValid()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Configuration is not valid");
    }
    if (wellKnownUrl != null) {
      try {
        wellKnownDiscoveryConfig = fetchWellKnown(URI.create(wellKnownUrl).toURL());
      } catch (MalformedURLException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Wrong URL given for well-known endpoint " + wellKnownUrl);
      }
      if (iss == null) {
        iss = wellKnownDiscoveryConfig.getIssuer();
      }
      if (jwksUrl == null) {
        jwksUrl = List.of(wellKnownDiscoveryConfig.getJwksUrl());
      }
      if (authorizationEndpoint == null) {
        authorizationEndpoint = wellKnownDiscoveryConfig.getAuthorizationEndpoint();
      }

      if (tokenEndpoint == null) {
        tokenEndpoint = wellKnownDiscoveryConfig.getTokenEndpoint();
      }
    }
    if (iss == null && usesHttpsJwk() && !JWTAuthPlugin.PRIMARY_ISSUER.equals(name)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Missing required config 'iss' for issuer " + getName());
    }
  }

  /**
   * Parses configuration for one IssuerConfig and sets all variables found
   *
   * @throws SolrException if unknown parameter names found in config
   */
  protected void parseConfigMap(Map<String, Object> configMap) {
    HashMap<String, Object> conf = new HashMap<>(configMap); // Clone
    setName((String) conf.get(PARAM_ISS_NAME));
    setWellKnownUrl((String) conf.get(PARAM_WELL_KNOWN_URL));
    setIss((String) conf.get(PARAM_ISSUER));
    setClientId((String) conf.get(PARAM_CLIENT_ID));
    setAud((String) conf.get(PARAM_AUDIENCE));
    Object confJwksUrl = conf.get(PARAM_JWKS_URL);
    setJwksUrl(confJwksUrl);
    setJsonWebKeySet(conf.get(PARAM_JWK));
    setAuthorizationEndpoint((String) conf.get(PARAM_AUTHORIZATION_ENDPOINT));
    setTokenEndpoint((String) conf.get(PARAM_TOKEN_ENDPOINT));
    setAuthorizationFlow((String) conf.get(PARAM_AUTHORIZATION_FLOW));

    conf.remove(PARAM_WELL_KNOWN_URL);
    conf.remove(PARAM_ISSUER);
    conf.remove(PARAM_ISS_NAME);
    conf.remove(PARAM_CLIENT_ID);
    conf.remove(PARAM_AUDIENCE);
    conf.remove(PARAM_JWKS_URL);
    conf.remove(PARAM_JWK);
    conf.remove(PARAM_AUTHORIZATION_ENDPOINT);
    conf.remove(PARAM_TOKEN_ENDPOINT);
    conf.remove(PARAM_AUTHORIZATION_FLOW);

    if (!conf.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unknown configuration key " + conf.keySet() + " for issuer " + name);
    }
  }

  /**
   * Setter that takes a jwk config object, parses it into a {@link JWKSet} and sets it
   *
   * @param jwksObject the config object to parse
   */
  @SuppressWarnings("unchecked")
  protected void setJsonWebKeySet(Object jwksObject) {
    try {
      if (jwksObject != null) {
        jsonWebKeySet = parseJwkSet((Map<String, Object>) jwksObject);
      }
    } catch (ParseException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed parsing parameter 'jwk' for issuer " + getName(),
          e);
    }
  }

  @SuppressWarnings("unchecked")
  protected static JWKSet parseJwkSet(Map<String, Object> jwkObj) throws ParseException {
    String json = Utils.toJSONString(jwkObj);
    if (jwkObj.containsKey("keys")) {
      return JWKSet.parse(json);
    } else {
      return new JWKSet(JWK.parse(json));
    }
  }

  private WellKnownDiscoveryConfig fetchWellKnown(URL wellKnownUrl) {
    return WellKnownDiscoveryConfig.parse(wellKnownUrl, trustedCerts);
  }

  public String getIss() {
    return iss;
  }

  public JWTIssuerConfig setIss(String iss) {
    this.iss = iss;
    return this;
  }

  public String getName() {
    return name;
  }

  public JWTIssuerConfig setName(String name) {
    this.name = name;
    return this;
  }

  public String getWellKnownUrl() {
    return wellKnownUrl;
  }

  public JWTIssuerConfig setWellKnownUrl(String wellKnownUrl) {
    this.wellKnownUrl = wellKnownUrl;
    return this;
  }

  public List<String> getJwksUrls() {
    return jwksUrl;
  }

  public JWTIssuerConfig setJwksUrl(List<String> jwksUrl) {
    this.jwksUrl = jwksUrl;
    return this;
  }

  /**
   * Setter that converts from String or List into a list
   *
   * @param jwksUrlListOrString object that should be either string or list
   * @return this for builder pattern
   * @throws SolrException if wrong type
   */
  @SuppressWarnings("unchecked")
  public JWTIssuerConfig setJwksUrl(Object jwksUrlListOrString) {
    if (jwksUrlListOrString instanceof String) this.jwksUrl = List.of((String) jwksUrlListOrString);
    else if (jwksUrlListOrString instanceof List) this.jwksUrl = (List<String>) jwksUrlListOrString;
    else if (jwksUrlListOrString != null)
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Parameter " + PARAM_JWKS_URL + " must be either List or String");
    return this;
  }

  public List<JwkSetFetcher> getHttpsJwks() {
    if (httpsJwks == null) {
      httpsJwks = httpsJwksFactory.createList(getJwksUrls());
    }
    return httpsJwks;
  }

  /**
   * Set the factory to use when creating JwkSetFetcher objects
   *
   * @param httpsJwksFactory factory with custom settings
   */
  public static void setHttpsJwksFactory(HttpsJwksFactory httpsJwksFactory) {
    JWTIssuerConfig.httpsJwksFactory = httpsJwksFactory;
  }

  public JWKSet getJsonWebKeySet() {
    return jsonWebKeySet;
  }

  public JWTIssuerConfig setJsonWebKeySet(JWKSet jsonWebKeySet) {
    this.jsonWebKeySet = jsonWebKeySet;
    return this;
  }

  /**
   * Check if the issuer is backed by HttpsJwk url(s)
   *
   * @return true if keys are fetched over https
   */
  public boolean usesHttpsJwk() {
    return getJwksUrls() != null && !getJwksUrls().isEmpty();
  }

  public WellKnownDiscoveryConfig getWellKnownDiscoveryConfig() {
    return wellKnownDiscoveryConfig;
  }

  public String getAud() {
    return aud;
  }

  public JWTIssuerConfig setAud(String aud) {
    this.aud = aud;
    return this;
  }

  public String getClientId() {
    return clientId;
  }

  public JWTIssuerConfig setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public String getAuthorizationEndpoint() {
    return authorizationEndpoint;
  }

  public JWTIssuerConfig setAuthorizationEndpoint(String authorizationEndpoint) {
    this.authorizationEndpoint = authorizationEndpoint;
    return this;
  }

  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  public JWTIssuerConfig setTokenEndpoint(String tokenEndpoint) {
    this.tokenEndpoint = tokenEndpoint;
    return this;
  }

  public String getAuthorizationFlow() {
    return authorizationFlow;
  }

  public JWTIssuerConfig setAuthorizationFlow(String authorizationFlow) {
    this.authorizationFlow =
        StrUtils.isNullOrEmpty(authorizationFlow)
            ? DEFAULT_AUTHORIZATION_FLOW
            : authorizationFlow.trim();
    if (!VALID_AUTHORIZATION_FLOWS.contains(this.authorizationFlow)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Invalid value for "
              + PARAM_AUTHORIZATION_FLOW
              + ". Expected one of "
              + VALID_AUTHORIZATION_FLOWS
              + " but found "
              + authorizationFlow);
    }
    if (this.authorizationFlow.equals("implicit")) {
      log.warn(
          "JWT authentication plugin is using 'implicit flow' which is deprecated and less secure. It's recommended to switch to 'code_pkce'");
    }
    return this;
  }

  public Map<String, Object> asConfig() {
    HashMap<String, Object> config = new HashMap<>();
    putIfNotNull(config, PARAM_ISS_NAME, name);
    putIfNotNull(config, PARAM_ISSUER, iss);
    putIfNotNull(config, PARAM_AUDIENCE, aud);
    putIfNotNull(config, PARAM_JWKS_URL, jwksUrl);
    putIfNotNull(config, PARAM_WELL_KNOWN_URL, wellKnownUrl);
    putIfNotNull(config, PARAM_CLIENT_ID, clientId);
    putIfNotNull(config, PARAM_AUTHORIZATION_ENDPOINT, authorizationEndpoint);
    putIfNotNull(config, PARAM_TOKEN_ENDPOINT, tokenEndpoint);
    putIfNotNull(config, PARAM_AUTHORIZATION_FLOW, authorizationFlow);
    if (jsonWebKeySet != null) {
      Map<String, Object> jwkSetMap = new HashMap<>();
      jwkSetMap.put(
          "keys",
          jsonWebKeySet.getKeys().stream().map(JWK::toJSONObject).collect(Collectors.toList()));
      putIfNotNull(config, PARAM_JWK, jwkSetMap);
    }
    return config;
  }

  private void putIfNotNull(HashMap<String, Object> config, String paramName, Object value) {
    if (value != null) {
      config.put(paramName, value);
    }
  }

  /**
   * Validates that this config has a name and either jwksUrl, wellkKownUrl or jwk
   *
   * @return true if a configuration is found and is valid, otherwise false
   * @throws SolrException if configuration is present but wrong
   */
  public boolean isValid() {
    int jwkConfigured = wellKnownUrl != null ? 1 : 0;
    jwkConfigured += jwksUrl != null ? 2 : 0;
    jwkConfigured += jsonWebKeySet != null ? 2 : 0;
    if (jwkConfigured > 3) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "JWTAuthPlugin needs to configure exactly one of "
              + PARAM_WELL_KNOWN_URL
              + ", "
              + PARAM_JWKS_URL
              + " and "
              + PARAM_JWK);
    }
    if (jwkConfigured > 0 && name == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Parameter 'name' is required for issuer configurations");
    }
    return jwkConfigured > 0;
  }

  public void setTrustedCerts(Collection<X509Certificate> trustedCerts) {
    this.trustedCerts = trustedCerts;
  }

  @VisibleForTesting
  public Collection<X509Certificate> getTrustedCerts() {
    return this.trustedCerts;
  }

  /** Builds an SSL socket factory trusting the given certificates. */
  static SSLSocketFactory buildSSLSocketFactory(Collection<X509Certificate> trustedCerts) {
    try {
      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(null, null);
      int i = 0;
      for (X509Certificate cert : trustedCerts) {
        ks.setCertificateEntry("trusted-cert-" + i++, cert);
      }
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, tmf.getTrustManagers(), null);
      return sslContext.getSocketFactory();
    } catch (GeneralSecurityException | IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Failed to build custom SSL context", e);
    }
  }

  /**
   * Builds a ResourceRetriever with optional custom SSL trust store and localhost hostname bypass.
   * Uses a custom implementation when trusted certs or localhost hostname bypass is needed, and the
   * default DefaultResourceRetriever otherwise.
   */
  static ResourceRetriever buildResourceRetriever(
      Collection<X509Certificate> trustedCerts, URL url) {
    if (trustedCerts == null) {
      return new DefaultResourceRetriever();
    }
    SSLSocketFactory ssf = buildSSLSocketFactory(trustedCerts);
    InetAddress loopback = InetAddress.getLoopbackAddress();
    boolean disableHostnameVerification =
        loopback.getCanonicalHostName().equals(url.getHost())
            || loopback.getHostName().equals(url.getHost());
    return resourceUrl -> {
      URLConnection conn = resourceUrl.openConnection();
      if (conn instanceof HttpsURLConnection httpsConn) {
        httpsConn.setSSLSocketFactory(ssf);
        if (disableHostnameVerification) {
          httpsConn.setHostnameVerifier((hostname, session) -> true);
        }
      }
      try (InputStream in = conn.getInputStream()) {
        String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        return new Resource(content, conn.getContentType());
      }
    };
  }

  /** Fetches and caches a JWK set from a remote URL using nimbus-jose-jwt's ResourceRetriever. */
  public static class JwkSetFetcher {
    private final String url;
    private final ResourceRetriever retriever;
    private final long cacheDurationSeconds;
    private final long refreshReprieveThresholdMs;
    private JWKSet cachedSet;
    private Instant cacheExpiry = Instant.EPOCH;
    private Instant lastRefreshTime = Instant.EPOCH;

    JwkSetFetcher(
        String url,
        ResourceRetriever retriever,
        long cacheDurationSeconds,
        long refreshReprieveThresholdMs) {
      this.url = url;
      this.retriever = retriever;
      this.cacheDurationSeconds = cacheDurationSeconds;
      this.refreshReprieveThresholdMs = refreshReprieveThresholdMs;
    }

    public synchronized List<JWK> getKeys() throws IOException, ParseException {
      if (cachedSet == null || Instant.now().isAfter(cacheExpiry)) {
        refresh();
      }
      return cachedSet.getKeys();
    }

    /**
     * Fetches fresh keys from the remote JWK endpoint. Calls within the refresh reprieve window are
     * ignored to avoid hammering the IdP on repeated unknown-key requests.
     */
    public synchronized void refresh() throws IOException, ParseException {
      Instant now = Instant.now();
      if (cachedSet != null
          && now.isBefore(lastRefreshTime.plusMillis(refreshReprieveThresholdMs))) {
        return;
      }
      try {
        Resource resource = retriever.retrieveResource(URI.create(url).toURL());
        cachedSet = JWKSet.parse(resource.getContent());
        cacheExpiry = now.plusSeconds(cacheDurationSeconds);
        lastRefreshTime = now;
      } catch (MalformedURLException e) {
        throw new IOException("Malformed JWK URL: " + url, e);
      }
    }

    public String getLocation() {
      return url;
    }
  }

  public static class HttpsJwksFactory {
    private final long jwkCacheDuration;
    private final long refreshReprieveThreshold;
    private Collection<X509Certificate> trustedCerts;

    public HttpsJwksFactory(long jwkCacheDuration, long refreshReprieveThreshold) {
      this.jwkCacheDuration = jwkCacheDuration;
      this.refreshReprieveThreshold = refreshReprieveThreshold;
    }

    public HttpsJwksFactory(
        long jwkCacheDuration,
        long refreshReprieveThreshold,
        Collection<X509Certificate> trustedCerts) {
      this.jwkCacheDuration = jwkCacheDuration;
      this.refreshReprieveThreshold = refreshReprieveThreshold;
      this.trustedCerts = trustedCerts;
    }

    private JwkSetFetcher create(String url) {
      final URL jwksUrl;
      try {
        jwksUrl = URI.create(url).toURL();
        checkAllowOutboundHttpConnections(PARAM_JWKS_URL, jwksUrl);
      } catch (MalformedURLException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Url " + url + " configured in " + PARAM_JWKS_URL + " is not a valid URL");
      }

      ResourceRetriever retriever = buildResourceRetriever(trustedCerts, jwksUrl);
      return new JwkSetFetcher(url, retriever, jwkCacheDuration, refreshReprieveThreshold);
    }

    public List<JwkSetFetcher> createList(List<String> jwkUrls) {
      return jwkUrls.stream().map(this::create).collect(Collectors.toList());
    }
  }

  /**
   * Config object for a OpenId Connect well-known config.
   *
   * <p>Typically exposed through <code>/.well-known/openid-configuration endpoint</code>.
   */
  public static class WellKnownDiscoveryConfig {
    private final Map<String, Object> securityConf;

    WellKnownDiscoveryConfig(Map<String, Object> securityConf) {
      this.securityConf = securityConf;
    }

    public static WellKnownDiscoveryConfig parse(String urlString) throws MalformedURLException {
      return parse(URI.create(urlString).toURL(), null);
    }

    /**
     * Fetch well-known config from a URL, with optional list of trusted certificates
     *
     * @param url the url to fetch
     * @param trustedCerts optional list of trusted SSL certs. May be null to fall-back to Java's
     *     defaults
     * @return an instance of WellKnownDiscoveryConfig object
     */
    public static WellKnownDiscoveryConfig parse(
        URL url, Collection<X509Certificate> trustedCerts) {
      try {
        if (!Arrays.asList("https", "file", "http").contains(url.getProtocol())) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Well-known config URL must be one of HTTPS or HTTP or file");
        }
        checkAllowOutboundHttpConnections(PARAM_WELL_KNOWN_URL, url);

        if ("file".equals(url.getProtocol())) {
          return parse(url.openStream());
        } else {
          ResourceRetriever retriever = buildResourceRetriever(trustedCerts, url);
          Resource resp = retriever.retrieveResource(url);
          return parse(
              new ByteArrayInputStream(resp.getContent().getBytes(StandardCharsets.UTF_8)));
        }
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Well-known config could not be read from url " + url,
            e);
      }
    }

    @VisibleForTesting
    public static WellKnownDiscoveryConfig parse(String json, Charset charset) {
      return parse(new ByteArrayInputStream(json.getBytes(charset)));
    }

    @SuppressWarnings("unchecked")
    public static WellKnownDiscoveryConfig parse(InputStream configStream) {
      return new WellKnownDiscoveryConfig((Map<String, Object>) Utils.fromJSON(configStream));
    }

    public String getJwksUrl() {
      return (String) securityConf.get("jwks_uri");
    }

    public String getIssuer() {
      return (String) securityConf.get("issuer");
    }

    public String getAuthorizationEndpoint() {
      return (String) securityConf.get("authorization_endpoint");
    }

    public String getUserInfoEndpoint() {
      return (String) securityConf.get("userinfo_endpoint");
    }

    public String getTokenEndpoint() {
      return (String) securityConf.get("token_endpoint");
    }

    @SuppressWarnings("unchecked")
    public List<String> getScopesSupported() {
      return (List<String>) securityConf.get("scopes_supported");
    }

    @SuppressWarnings("unchecked")
    public List<String> getResponseTypesSupported() {
      return (List<String>) securityConf.get("response_types_supported");
    }
  }

  public static void checkAllowOutboundHttpConnections(String parameterName, URL url) {
    if ("http".equalsIgnoreCase(url.getProtocol())) {
      if (!ALLOW_OUTBOUND_HTTP) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            parameterName + " is using http protocol. " + ALLOW_OUTBOUND_HTTP_ERR_MSG);
      }
    }
  }
}

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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.Principal;
import java.security.PublicKey;
import java.security.SignatureException;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpListenerFactory;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.CryptoKeys;
import org.eclipse.jetty.client.api.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PKIAuthenticationPlugin extends AuthenticationPlugin
    implements HttpClientBuilderPlugin {

  public static final String ACCEPT_VERSIONS = "solr.pki.acceptVersions";
  public static final String SEND_VERSION = "solr.pki.sendVersion";

  /**
   * Mark the current thread as a server thread and set a flag in SolrRequestInfo to indicate you
   * want to send a request as the server identity instead of as the authenticated user.
   *
   * @param enabled If true, enable the current thread to make requests with the server identity.
   * @see SolrRequestInfo#setUseServerToken(boolean)
   */
  public static void withServerIdentity(final boolean enabled) {
    SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
    if (requestInfo != null) {
      requestInfo.setUseServerToken(enabled);
    }
    ExecutorUtil.setServerThreadFlag(enabled ? enabled : null);
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** If a number has less than this number of digits, it'll not be considered a timestamp. */
  private static final int MIN_TIMESTAMP_DIGITS = 10; // a timestamp of 9999999999 is year 1970
  /** If a number has more than this number of digits, it'll not be considered a timestamp. */
  private static final int MAX_TIMESTAMP_DIGITS = 13; // a timestamp of 9999999999999 is year 2286

  private final Map<String, PublicKey> keyCache = new ConcurrentHashMap<>();
  private final PublicKeyHandler publicKeyHandler;
  private final CoreContainer cores;
  private static final int MAX_VALIDITY = Integer.getInteger("pkiauth.ttl", 5000);
  private final String myNodeName;
  private final HttpHeaderClientInterceptor interceptor = new HttpHeaderClientInterceptor();
  private boolean interceptorRegistered = false;

  private boolean acceptPkiV1 = false;

  public boolean isInterceptorRegistered() {
    return interceptorRegistered;
  }

  // TODO We only use PublicKeyHandler here to gain access to the underlying keypair; let's nuke the
  // indirection and
  //  just pass in the SolrNodeKeyPair instance directly
  public PKIAuthenticationPlugin(
      CoreContainer cores, String nodeName, PublicKeyHandler publicKeyHandler) {
    this.publicKeyHandler = publicKeyHandler;
    this.cores = cores;
    myNodeName = nodeName;

    Set<String> knownPkiVersions = Set.of("v1", "v2");
    // We always accept v2 even if it is not specified
    String[] versions = System.getProperty(ACCEPT_VERSIONS, "v2").split(",");
    for (String version : versions) {
      if (knownPkiVersions.contains(version) == false) {
        log.warn("Unknown protocol version [{}] specified in {}", version, ACCEPT_VERSIONS);
      }
      if ("v1".equals(version)) {
        log.warn(
            "System setting {} includes the deprecated v1, which should only be used for compatibility during rolling upgrades. "
                + "After all servers have been upgraded, consider disabling this compatability layer.",
            ACCEPT_VERSIONS);
        acceptPkiV1 = true;
      }
    }
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {}

  @SuppressForbidden(reason = "Needs currentTimeMillis to compare against time in header")
  @Override
  public boolean doAuthenticate(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws Exception {
    // Getting the received time must be the first thing we do, processing the request can take time
    long receivedTime = System.currentTimeMillis();

    String requestURI = request.getRequestURI();
    if (requestURI.endsWith(PublicKeyHandler.PATH)) {
      assert false : "Should already be handled by SolrDispatchFilter.authenticateRequest";

      numPassThrough.inc();
      filterChain.doFilter(request, response);
      return true;
    }

    PKIHeaderData headerData = null;
    String headerV2 = request.getHeader(HEADER_V2);
    String headerV1 = request.getHeader(HEADER);
    if (headerV2 != null) {
      // Try V2 first
      int nodeNameEnd = headerV2.indexOf(' ');
      if (nodeNameEnd <= 0) {
        // Do not log the value as it is likely gibberish
        return sendError(response, true, "Could not parse node name from SolrAuthV2 header.");
      }

      headerData = decipherHeaderV2(headerV2, headerV2.substring(0, nodeNameEnd));
    } else if (headerV1 != null && acceptPkiV1) {
      List<String> authInfo = StrUtils.splitWS(headerV1, false);
      if (authInfo.size() != 2) {
        // We really shouldn't be logging and returning this, but we did it before so keep that
        return sendError(response, false, "Invalid SolrAuth header: " + headerV1);
      }
      headerData = decipherHeader(authInfo.get(0), authInfo.get(1));
    }

    if (headerData == null) {
      return sendError(response, true, "Could not load principal from SolrAuthV2 header.");
    }
    long elapsed = receivedTime - headerData.timestamp;
    if (elapsed > MAX_VALIDITY) {
      return sendError(response, true, "Expired key request timestamp, elapsed=" + elapsed);
    }

    final Principal principal =
        "$".equals(headerData.userName)
            ? CLUSTER_MEMBER_NODE
            : new BasicUserPrincipal(headerData.userName);

    numAuthenticated.inc();
    filterChain.doFilter(wrapWithPrincipal(request, principal), response);
    return true;
  }

  /**
   * Set the response header errors, possibly log something and return false for failed
   * authentication
   *
   * @param response the response to set error status with
   * @param v2 whether this authentication used the v1 or v2 header (true if v2)
   * @param message the message to log and send back to client. do not include anyhting sensitive
   *     here about server state
   * @return false to chain with calls from authenticate
   */
  private boolean sendError(HttpServletResponse response, boolean v2, String message)
      throws IOException {
    numErrors.mark();
    log.error(message);
    response.setHeader(HttpHeaders.WWW_AUTHENTICATE, v2 ? HEADER_V2 : HEADER);
    response.sendError(HttpServletResponse.SC_UNAUTHORIZED, message);
    return false;
  }

  public static class PKIHeaderData {
    String userName;
    long timestamp;

    @Override
    public String toString() {
      return "PKIHeaderData{" + "userName='" + userName + '\'' + ", timestamp=" + timestamp + '}';
    }
  }

  private PKIHeaderData decipherHeaderV2(String header, String nodeName) {
    PublicKey key = keyCache.get(nodeName);
    if (key == null) {
      log.debug("No key available for node: {} fetching now ", nodeName);
      key = getRemotePublicKey(nodeName);
      log.debug("public key obtained {} ", key);
    }

    int sigStart = header.lastIndexOf(' ');

    String data = header.substring(0, sigStart);
    byte[] sig = Base64.getDecoder().decode(header.substring(sigStart + 1));
    PKIHeaderData rv = validateSignature(data, sig, key, false);
    if (rv == null) {
      log.warn("Failed to verify signature, trying after refreshing the key ");
      key = getRemotePublicKey(nodeName);
      rv = validateSignature(data, sig, key, true);
    }

    return rv;
  }

  private PKIHeaderData validateSignature(String data, byte[] sig, PublicKey key, boolean isRetry) {
    try {
      if (CryptoKeys.verifySha256(data.getBytes(UTF_8), sig, key)) {
        int timestampStart = data.lastIndexOf(' ');
        PKIHeaderData rv = new PKIHeaderData();
        String ts = data.substring(timestampStart + 1);
        try {
          rv.timestamp = Long.parseLong(ts);
        } catch (NumberFormatException e) {
          log.error("SolrAuthV2 header error, cannot parse {} as timestamp", ts);
          return null;
        }
        rv.userName = data.substring(data.indexOf(' ') + 1, timestampStart);
        return rv;
      } else {
        log.warn("Signature verification failed, signature or checksum does not match");
        return null;
      }
    } catch (InvalidKeyException | SignatureException e) {
      if (isRetry) {
        log.error("Signature validation on retry failed, likely key error");
      } else {
        log.info("Signature validation failed first attempt, likely key error");
      }
      return null;
    }
  }

  private PKIHeaderData decipherHeader(String nodeName, String cipherBase64) {
    PublicKey key = keyCache.get(nodeName);
    if (key == null) {
      log.debug("No key available for node: {} fetching now ", nodeName);
      key = getRemotePublicKey(nodeName);
      log.debug("public key obtained {} ", key);
    }

    PKIHeaderData header = parseCipher(cipherBase64, key, false);
    if (header == null) {
      log.warn("Failed to decrypt header, trying after refreshing the key ");
      key = getRemotePublicKey(nodeName);
      return parseCipher(cipherBase64, key, true);
    } else {
      return header;
    }
  }

  @VisibleForTesting
  static PKIHeaderData parseCipher(String cipher, PublicKey key, boolean isRetry) {
    byte[] bytes;
    try {
      bytes = CryptoKeys.decryptRSA(Base64.getDecoder().decode(cipher), key);
    } catch (Exception e) {
      if (isRetry) {
        log.error("Decryption failed on retry, key must be wrong", e);
      } else {
        log.info("Decryption failed on first attempt, will retry", e);
      }
      return null;
    }
    String s = new String(bytes, UTF_8).trim();
    int splitPoint = s.lastIndexOf(' ');
    int timestampDigits = s.length() - 1 - splitPoint;
    if (splitPoint == -1
        || timestampDigits < MIN_TIMESTAMP_DIGITS
        || timestampDigits > MAX_TIMESTAMP_DIGITS) {
      log.warn("Invalid cipher {} deciphered data {}", cipher, s);
      return null;
    }
    PKIHeaderData headerData = new PKIHeaderData();
    try {
      headerData.timestamp = Long.parseLong(s.substring(splitPoint + 1));
      headerData.userName = s.substring(0, splitPoint);
      log.debug("Successfully decrypted header {} {}", headerData.userName, headerData.timestamp);
      return headerData;
    } catch (NumberFormatException e) {
      log.warn("Invalid cipher {}", cipher);
      return null;
    }
  }

  /**
   * Fetch the public key for a remote Solr node and store it in our key cache, replacing any
   * existing entries.
   *
   * @param nodename the node to fetch a key from
   * @return the public key
   */
  PublicKey getRemotePublicKey(String nodename) {
    if (!cores
        .getZkController()
        .getZkStateReader()
        .getClusterState()
        .getLiveNodes()
        .contains(nodename)) return null;
    String url = cores.getZkController().getZkStateReader().getBaseUrlForNodeName(nodename);
    HttpEntity entity = null;
    try {
      String uri = url + PublicKeyHandler.PATH + "?wt=json&omitHeader=true";
      log.debug("Fetching fresh public key from: {}", uri);
      HttpResponse rsp =
          cores
              .getUpdateShardHandler()
              .getDefaultHttpClient()
              .execute(new HttpGet(uri), HttpClientUtil.createNewHttpClientRequestContext());
      entity = rsp.getEntity();
      byte[] bytes = EntityUtils.toByteArray(entity);
      Map<?, ?> m = (Map<?, ?>) Utils.fromJSON(bytes);
      String key = (String) m.get("key");
      if (key == null) {
        log.error("No key available from {}{}", url, PublicKeyHandler.PATH);
        return null;
      } else {
        log.info("New key obtained from node={}, key={}", nodename, key);
      }
      PublicKey pubKey = CryptoKeys.deserializeX509PublicKey(key);
      keyCache.put(nodename, pubKey);
      return pubKey;
    } catch (Exception e) {
      log.error("Exception trying to get public key from: {}", url, e);
      return null;
    } finally {
      Utils.consumeFully(entity);
    }
  }

  @Override
  public void setup(Http2SolrClient client) {
    final HttpListenerFactory.RequestResponseListener listener =
        new HttpListenerFactory.RequestResponseListener() {
          private static final String CACHED_REQUEST_USER_KEY = "cachedRequestUser";

          @Override
          public void onQueued(Request request) {
            log.trace("onQueued: {}", request);
            if (cores.getAuthenticationPlugin() == null) {
              log.trace("no authentication plugin, skipping");
              return;
            }
            if (!cores.getAuthenticationPlugin().interceptInternodeRequest(request)) {
              if (log.isDebugEnabled()) {
                log.debug("{} secures this internode request", this.getClass().getSimpleName());
              }

              // The onBegin hook below (potentially) runs in a separate Jetty thread than was
              // used to submit the request.  While we're still in the submitting thread, fetch
              // the user information from the SolrRequestInfo thread local and cache it on the
              // Request so it can be accessed accurately in onBegin
              cachePreFetchedUserOnJettyRequest(request);
            } else {
              if (log.isDebugEnabled()) {
                log.debug(
                    "{} secures this internode request",
                    cores.getAuthenticationPlugin().getClass().getSimpleName());
              }
            }
          }

          @Override
          public void onBegin(Request request) {
            log.trace("onBegin: {}", request);

            final Optional<String> preFetchedUser = getUserFromJettyRequest(request);
            if ("v1".equals(System.getProperty(SEND_VERSION))) {
              preFetchedUser
                  .map(PKIAuthenticationPlugin.this::generateToken)
                  .ifPresent(token -> request.header(HEADER, token));
            } else {
              preFetchedUser
                  .map(PKIAuthenticationPlugin.this::generateTokenV2)
                  .ifPresent(token -> request.header(HEADER_V2, token));
            }
          }

          private void cachePreFetchedUserOnJettyRequest(Request request) {
            getUser().ifPresent(user -> request.attribute(CACHED_REQUEST_USER_KEY, user));
          }

          private Optional<String> getUserFromJettyRequest(Request request) {
            return Optional.ofNullable(
                (String) request.getAttributes().get(CACHED_REQUEST_USER_KEY));
          }
        };
    client.addListenerFactory(() -> listener);
  }

  @Override
  public SolrHttpClientBuilder getHttpClientBuilder(SolrHttpClientBuilder builder) {
    HttpClientUtil.addRequestInterceptor(interceptor);
    interceptorRegistered = true;
    return builder;
  }

  public boolean needsAuthorization(HttpServletRequest req) {
    return req.getUserPrincipal() != CLUSTER_MEMBER_NODE;
  }

  private class HttpHeaderClientInterceptor implements HttpRequestInterceptor {

    public HttpHeaderClientInterceptor() {}

    @Override
    public void process(HttpRequest httpRequest, HttpContext httpContext)
        throws HttpException, IOException {
      if (cores.getAuthenticationPlugin() == null) {
        return;
      }
      if (!cores.getAuthenticationPlugin().interceptInternodeRequest(httpRequest, httpContext)) {
        if (log.isDebugEnabled()) {
          log.debug("{} secures this internode request", this.getClass().getSimpleName());
        }
        setHeader(httpRequest);
      } else {
        if (log.isDebugEnabled()) {
          log.debug(
              "{} secures this internode request",
              cores.getAuthenticationPlugin().getClass().getSimpleName());
        }
      }
    }
  }

  private Optional<String> getUser() {
    SolrRequestInfo reqInfo = getRequestInfo();
    if (reqInfo != null && !reqInfo.useServerToken()) {
      Principal principal = reqInfo.getUserPrincipal();
      if (principal == null) {
        log.debug("generateToken: principal is null");
        // this had a request but not authenticated
        // so we don't not need to set a principal
        return Optional.empty();
      } else {
        assert principal.getName() != null;
        return Optional.of(principal.getName());
      }
    } else {
      if (!isSolrThread()) {
        // if this is not running inside a Solr threadpool (as in testcases)
        // then no need to add any header
        log.debug("generateToken: not a solr (server) thread");
        return Optional.empty();
      }
      // this request seems to be originated from Solr itself
      return Optional.of(NODE_IS_USER); // special name to denote the user is the node itself
    }
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to set current time in header")
  private String generateToken(String usr) {
    assert usr != null;
    String s = usr + " " + System.currentTimeMillis();
    byte[] payload = s.getBytes(UTF_8);
    byte[] payloadCipher = publicKeyHandler.getKeyPair().encrypt(ByteBuffer.wrap(payload));
    String base64Cipher = Base64.getEncoder().encodeToString(payloadCipher);
    log.trace("generateToken: usr={} token={}", usr, base64Cipher);
    return myNodeName + " " + base64Cipher;
  }

  private String generateTokenV2(String user) {
    assert user != null;
    String s = myNodeName + " " + user + " " + Instant.now().toEpochMilli();

    byte[] payload = s.getBytes(UTF_8);
    byte[] signature = publicKeyHandler.getKeyPair().signSha256(payload);
    String base64Signature = Base64.getEncoder().encodeToString(signature);
    return s + " " + base64Signature;
  }

  void setHeader(HttpRequest httpRequest) {
    if ("v1".equals(System.getProperty(SEND_VERSION))) {
      getUser().map(this::generateToken).ifPresent(token -> httpRequest.setHeader(HEADER, token));
    } else {
      getUser()
          .map(this::generateTokenV2)
          .ifPresent(token -> httpRequest.setHeader(HEADER_V2, token));
    }
  }

  boolean isSolrThread() {
    return ExecutorUtil.isSolrServerThread();
  }

  SolrRequestInfo getRequestInfo() {
    return SolrRequestInfo.getRequestInfo();
  }

  @Override
  public void close() throws IOException {
    HttpClientUtil.removeRequestInterceptor(interceptor);
    interceptorRegistered = false;
  }

  @VisibleForTesting
  public String getPublicKey() {
    return publicKeyHandler.getKeyPair().getPublicKeyStr();
  }

  public static final String HEADER = "SolrAuth";
  public static final String HEADER_V2 = "SolrAuthV2";
  public static final String NODE_IS_USER = "$";
  // special principal to denote the cluster member
  public static final Principal CLUSTER_MEMBER_NODE = new BasicUserPrincipal("$");
}

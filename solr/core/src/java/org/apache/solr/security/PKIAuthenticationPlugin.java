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
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.InvalidKeyException;
import java.security.Principal;
import java.security.PublicKey;
import java.security.SignatureException;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.jetty.HttpListenerFactory;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.CryptoKeys;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PKIAuthenticationPlugin extends AuthenticationPlugin
    implements HttpClientBuilderPlugin {

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

  private final Map<String, PublicKey> keyCache = new ConcurrentHashMap<>();
  private final PublicKeyHandler publicKeyHandler;
  private final CoreContainer cores;
  private final LoadingCache<String, PKIHeaderData> validatedHeaderCache;
  private final LoadingCache<String, String> generatedV2TokenCache;
  private static final int MAX_VALIDITY = Integer.getInteger("pkiauth.ttl", 10000);
  private final String myNodeName;
  private boolean interceptorRegistered = false;

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

    // Don't expire after read, because there is no reason to add the overhead of updating expiry
    // information after each read. The expiration time here doesn't matter too much, because we
    // still check the PKI Token TTL after fetching from the cache. We just want to make sure cache
    // entries are cleaned up regularly.
    validatedHeaderCache =
        Caffeine.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(MAX_VALIDITY, TimeUnit.MILLISECONDS)
            .build(this::decipherHeaderV2);
    // We must expire much earlier than the max validity, because these cached Auth tokens still
    // need to be sent to the server, which will validate the TTL. If we expire at maxValidity, the
    // TTL check will always fail before the cache entry is expired.
    long expireAfterTime = MAX_VALIDITY / 4;
    // Refreshing is done asynchronously, so we want to do it before expiration. This means that
    // requests are not synchronously blocked when generating new Auth tokens. However, the refresh
    // will only happen when the cached header is requested. Therefore, we want to give a long-ish
    // runway for requests to come in to trigger an asynchronous-refresh before expiry causes a
    // synchronous-refresh.
    long shouldRefreshTime = Math.max(1, expireAfterTime / 2);
    generatedV2TokenCache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .refreshAfterWrite(shouldRefreshTime, TimeUnit.MILLISECONDS)
            .expireAfterWrite(expireAfterTime, TimeUnit.MILLISECONDS)
            .build(this::generateTokenV2);
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

    String headerV2 = request.getHeader(HEADER_V2);
    if (headerV2 == null) {
      return sendError(response, "No PKI auth header was provided");
    }

    int nodeNameEnd = headerV2.indexOf(' ');
    if (nodeNameEnd <= 0) {
      // Do not log the value as it is likely gibberish
      return sendError(response, "Could not parse node name from SolrAuthV2 header.");
    }

    PKIHeaderData headerData = validatedHeaderCache.get(headerV2);

    if (headerData == null) {
      return sendError(response, "Could not validate PKI header.");
    }
    long elapsed = receivedTime - headerData.timestamp;
    if (elapsed > MAX_VALIDITY) {
      return sendError(response, "Expired key request timestamp, elapsed=" + elapsed);
    }

    final Principal principal =
        "$".equals(headerData.userName)
            ? CLUSTER_MEMBER_NODE
            : new SimplePrincipal(headerData.userName);

    numAuthenticated.inc();
    filterChain.doFilter(wrapWithPrincipal(request, principal), response);
    return true;
  }

  /**
   * Set the response header errors, possibly log something and return false for failed
   * authentication
   *
   * @param response the response to set error status with
   * @param message the message to log and send back to client. do not include anything sensitive
   *     here about server state
   * @return false to chain with calls from authenticate
   */
  private boolean sendError(HttpServletResponse response, String message) throws IOException {
    numErrors.inc();
    log.error(message);
    response.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HEADER_V2);
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

  private PublicKey getOrFetchPublicKey(String nodeName) {
    PublicKey key = keyCache.get(nodeName);
    if (key == null) {
      log.debug("No key available for node: {} fetching now ", nodeName);
      key = fetchPublicKeyFromRemote(nodeName);
      log.debug("public key obtained {} ", key);
    }

    return key;
  }

  private PKIHeaderData decipherHeaderV2(String header) {
    String nodeName = header.substring(0, header.indexOf(' '));
    PublicKey key = getOrFetchPublicKey(nodeName);

    int sigStart = header.lastIndexOf(' ');

    String data = header.substring(0, sigStart);
    byte[] sig = Base64.getDecoder().decode(header.substring(sigStart + 1));
    PKIHeaderData rv = validateSignature(data, sig, key, false);
    if (rv == null) {
      log.warn("Failed to verify signature, trying after refreshing the key ");
      key = fetchPublicKeyFromRemote(nodeName);
      rv = validateSignature(data, sig, key, true);
    }

    return rv;
  }

  private PKIHeaderData validateSignature(String data, byte[] sig, PublicKey key, boolean isRetry) {
    if (key == null) {
      log.warn("Key is null when attempting to validate signature; skipping...");
      return null;
    }

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
      final String excMessage = e.getMessage();
      if (isRetry) {
        log.error("Signature validation on retry failed, likely key error: {}", excMessage);
      } else {
        log.info("Signature validation failed first attempt, likely key error: {}", excMessage);
      }
      return null;
    }
  }

  private boolean isInLiveNodes(String nodeName) {
    return cores
        .getZkController()
        .getZkStateReader()
        .getClusterState()
        .getLiveNodes()
        .contains(nodeName);
  }

  /**
   * Fetch the public key for a remote Solr node and store it in our key cache, replacing any
   * existing entries.
   *
   * @param nodename the node to fetch a key from
   * @return the public key
   */
  PublicKey fetchPublicKeyFromRemote(String nodename) {
    if (!isInLiveNodes(nodename)) {
      log.warn(
          "Unable to fetch public key for {} as it does not appear to be a Solr 'live node'",
          nodename);
      return null;
    }
    String url = cores.getZkController().getZkStateReader().getBaseUrlForNodeName(nodename);
    try {
      final var solrParams = new ModifiableSolrParams();
      solrParams.add("wt", "json");
      solrParams.add("omitHeader", "true");

      final var request = new GenericSolrRequest(GET, PublicKeyHandler.PATH, solrParams);
      log.debug("Fetching fresh public key from: {}", url);
      var solrClient = cores.getDefaultHttpSolrClient();
      NamedList<Object> resp = solrClient.requestWithBaseUrl(url, request, null);

      String key = (String) resp.get("key");
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
    }
  }

  @Override
  public void setup(HttpJettySolrClient client) {
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
            preFetchedUser
                .map(generatedV2TokenCache::get)
                .ifPresent(
                    token -> request.headers(httpFields -> httpFields.add(HEADER_V2, token)));
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

  // CLUSTER_MEMBER_NODE is a unique sentinel, so identity comparison against it is intentional
  @SuppressWarnings("ReferenceEquality")
  public boolean needsAuthorization(HttpServletRequest req) {
    return req.getUserPrincipal() != CLUSTER_MEMBER_NODE;
  }

  private Optional<String> getUser() {
    SolrRequestInfo reqInfo = getRequestInfo();
    if (reqInfo != null && !reqInfo.useServerToken()) {
      Principal principal = reqInfo.getUserPrincipal();
      if (principal == null) {
        log.debug("generateToken: principal is null");
        // this had a request but not authenticated
        // so we do not need to set a principal
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

  private String generateTokenV2(String user) {
    assert user != null;
    String s = myNodeName + " " + user + " " + Instant.now().toEpochMilli();

    byte[] payload = s.getBytes(UTF_8);
    byte[] signature = publicKeyHandler.getKeyPair().signSha256(payload);
    String base64Signature = Base64.getEncoder().encodeToString(signature);
    return s + " " + base64Signature;
  }

  @VisibleForTesting
  void setHeader(BiConsumer<String, String> httpRequest) {
    getUser()
        .map(generatedV2TokenCache::get)
        .ifPresent(token -> httpRequest.accept(HEADER_V2, token));
  }

  boolean isSolrThread() {
    return ExecutorUtil.isSolrServerThread();
  }

  SolrRequestInfo getRequestInfo() {
    return SolrRequestInfo.getRequestInfo();
  }

  @Override
  public void close() throws IOException {
    interceptorRegistered = false;
    super.close();
  }

  @VisibleForTesting
  public String getPublicKey() {
    return publicKeyHandler.getKeyPair().getPublicKeyStr();
  }

  public static final String HEADER_V2 = "SolrAuthV2";
  public static final String NODE_IS_USER = "$";
  // special principal to denote the cluster member
  public static final Principal CLUSTER_MEMBER_NODE = new SimplePrincipal("$");
}

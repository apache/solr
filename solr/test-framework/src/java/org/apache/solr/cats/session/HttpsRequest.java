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

package org.apache.solr.cats.session;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.solr.cats.exec.Shell;
import org.apache.solr.cats.session.CatsCliHelpers.HttpResult;

/**
 * A GET over HTTPS, carrying the client-side options a shell test would express as {@code curl}
 * flags: a trust store ({@code --cacert}), credentials ({@code -u}) and the host to address the
 * program by.
 *
 * <p>The plain-HTTP {@link CatsCliHelpers#http} takes no configuration and so is a method; TLS
 * takes several options, hence a builder. Obtained from {@link CatsCliHelpers#https(String)}, and
 * what it returns is the same {@link HttpResult} a plain request returns.
 *
 * <p>This is what the {@code curl} lines of a TLS suite port to. There is no curl on a stock
 * Windows runner, so a test that shells out to one could only ever pass on bash.
 */
public final class HttpsRequest {

  private final int port;
  private final String pathAndQuery;
  private String host = "localhost";
  private Path trustStore;
  private String trustStorePassword;
  private String authorization;
  private String curlAuth;
  private String contentType;
  private String body;
  private final Map<String, String> vars;
  private final Set<String> echoedPrefixes;
  private final Transcript transcript;
  private final Shell shell;

  HttpsRequest(
      int port,
      String pathAndQuery,
      Map<String, String> vars,
      Set<String> echoedPrefixes,
      Transcript transcript,
      Shell shell) {
    this.port = port;
    this.pathAndQuery = pathAndQuery;
    this.vars = vars;
    this.echoedPrefixes = echoedPrefixes;
    this.transcript = transcript;
    this.shell = shell;
  }

  /**
   * Addresses the program by this name rather than {@code localhost}, which decides both the SNI
   * name sent and the name the server certificate is checked against.
   */
  public HttpsRequest host(String hostName) {
    this.host = hostName;
    return this;
  }

  /**
   * Trusts only the certificates in this PKCS12 store, as {@code curl --cacert} does. A store
   * holding just a private key entry works, that entry's own certificate being the trust anchor —
   * which is how the suites use a keystore as its own trust store.
   */
  public HttpsRequest trustStore(Path store, String password) {
    this.trustStore = store;
    this.trustStorePassword = password;
    return this;
  }

  /** Port of {@code curl -u user:password --basic}: credentials sent on the first request. */
  public HttpsRequest basicAuth(String user, String password) {
    this.authorization =
        "Basic "
            + Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    this.curlAuth = user + ":" + password;
    return this;
  }

  /**
   * Sends the request.
   *
   * <p>A request that never completed — a TLS handshake failure, a refused connection — comes back
   * with a status of -1 and the exception chain as the body, so {@link
   * HttpResult#assertConnectionFailed()} can assert on it and a failure message says why.
   */
  public HttpResult get() {
    return send("GET", HttpRequest.BodyPublishers.noBody());
  }

  /**
   * Sends the request as a POST with a text body: the port of {@code curl -H 'Content-Type:<type>'
   * --data-binary '<body>'} over TLS. Same failure contract as {@link #get()}.
   */
  public HttpResult post(String contentType, String body) {
    this.contentType = contentType;
    this.body = body;
    return send("POST", HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
  }

  private HttpResult send(String method, HttpRequest.BodyPublisher publisher) {
    String url = "https://" + host + ":" + port + pathAndQuery;
    HttpClient.Builder clientBuilder =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10));
    if (trustStore != null) {
      clientBuilder.sslContext(trustingOnly(trustStore, trustStorePassword));
    }
    try (HttpClient client = clientBuilder.build()) {
      HttpRequest.Builder request =
          HttpRequest.newBuilder(URI.create(url))
              .timeout(Duration.ofSeconds(30))
              .method(method, publisher);
      if (contentType != null) {
        request.header("Content-Type", contentType);
      }
      if (authorization != null) {
        request.header("Authorization", authorization);
      }
      HttpResponse<String> response =
          client.send(request.build(), HttpResponse.BodyHandlers.ofString());
      transcript.recordHttp(curl(url), response.statusCode(), response.body());
      return new HttpResult(
          response.statusCode(), response.body(), response.headers(), vars, echoedPrefixes);
    } catch (IOException e) {
      transcript.recordHttp(curl(url), -1, describe(e));
      return new HttpResult(-1, describe(e), vars, echoedPrefixes);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted requesting " + url, e);
    }
  }

  /**
   * The curl command line this request ports back to, for the transcript: the {@code --cacert},
   * {@code -u}, {@code --resolve} and {@code --data-binary} the builder's options stand for.
   */
  private String curl(String url) {
    StringBuilder curl = new StringBuilder("curl");
    if (trustStore != null) {
      curl.append(" --cacert ").append(shell.quoteForDisplay(trustStore.toString()));
    }
    if (curlAuth != null) {
      curl.append(" -u ").append(shell.quoteForDisplay(curlAuth));
    }
    if (!"localhost".equals(host)) {
      curl.append(" --resolve ").append(shell.quoteForDisplay(host + ":" + port + ":127.0.0.1"));
    }
    if (body != null) {
      curl.append(" -H ").append(shell.quoteForDisplay("Content-Type: " + contentType));
      curl.append(" --data-binary ").append(shell.quoteForDisplay(body));
    }
    return curl.append(' ').append(shell.quoteForDisplay(url)).toString();
  }

  private static SSLContext trustingOnly(Path store, String password) {
    try (InputStream in = Files.newInputStream(store)) {
      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      keyStore.load(in, password == null ? null : password.toCharArray());
      TrustManagerFactory trust =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trust.init(keyStore);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, trust.getTrustManagers(), null);
      return context;
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException("Could not build an SSL context from " + store, e);
    }
  }

  /** The whole cause chain: the reason a handshake failed is usually two levels down. */
  private static String describe(Throwable failure) {
    StringBuilder chain = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      chain.append(chain.isEmpty() ? "" : "\nCaused by: ").append(cause);
    }
    return chain.toString();
  }
}

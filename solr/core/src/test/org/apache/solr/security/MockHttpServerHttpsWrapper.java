package org.apache.solr.security;

import kotlin.jvm.functions.Function1;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.PemUtils;
import no.nav.security.mock.oauth2.http.MockWebServerWrapper;
import no.nav.security.mock.oauth2.http.OAuth2HttpRequest;
import no.nav.security.mock.oauth2.http.OAuth2HttpResponse;
import no.nav.security.mock.oauth2.http.OAuth2HttpServer;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.solr.SolrTestCaseJ4;
import org.eclipse.jetty.http.HttpParser;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class MockHttpServerHttpsWrapper implements OAuth2HttpServer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private MockWebServer mockWebServer;

  public MockWebServer getMockWebServer() {
    return mockWebServer;
  }

  public MockHttpServerHttpsWrapper() {
    X509ExtendedKeyManager keyManager = PemUtils.loadIdentityMaterial(
        SolrTestCaseJ4.TEST_PATH().resolve("security").resolve("solr-ssl.pem"), "secret".toCharArray()
    );
    X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(
        SolrTestCaseJ4.TEST_PATH().resolve("security").resolve("solr-ssl.pem")
    );
    SSLFactory sslFactory = SSLFactory.builder()
        .withIdentityMaterial(keyManager)
        .withTrustMaterial(trustManager)
        .build();

    mockWebServer = new MockWebServer();
    try {
      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(sslFactory.getKeyManagerFactory().get().getKeyManagers(),
          sslFactory.getTrustManagerFactory().get().getTrustManagers(), null);
      SSLSocketFactory sf = sslContext.getSocketFactory();
      mockWebServer.useHttps(sf, false);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Failed initializing SSL for mock server: " + e.getMessage());
    }
  }

  @Override
  public void close() {
    try {
      mockWebServer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public int port() {
    return mockWebServer.getPort();
  }

  @NotNull
  @Override
  public OAuth2HttpServer start(@NotNull InetAddress inetAddress, int port, @NotNull Function1<? super OAuth2HttpRequest, OAuth2HttpResponse> requestHandler) {
    try {
      mockWebServer.start(inetAddress, port);
      log.debug("started server on address={} and port={}", inetAddress, mockWebServer.getPort());
      mockWebServer.setDispatcher(new MockWebServerDispatcher(requestHandler));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @NotNull
  @Override
  public OAuth2HttpServer start(int port, @NotNull Function1<? super OAuth2HttpRequest, OAuth2HttpResponse> requestHandler) {
    return null;
  }

  @NotNull
  @Override
  public OAuth2HttpServer start(@NotNull Function1<? super OAuth2HttpRequest, OAuth2HttpResponse> requestHandler) {
    return null;
  }

  @NotNull
  @Override
  public OAuth2HttpServer stop() {
    try {
      mockWebServer.shutdown();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @NotNull
  @Override
  public HttpUrl url(@NotNull String path) {
    return mockWebServer.url(path);
  }

  static class MockWebServerDispatcher extends Dispatcher {
    private final HttpParser.RequestHandler requestHandler;
    private final BlockingQueue<MockResponse> responseQueue;

    public MockWebServerDispatcher(HttpParser.RequestHandler requestHandler, BlockingQueue<MockResponse> responseQueue) {
      this.requestHandler = requestHandler;
      if (responseQueue == null) {
        this.responseQueue = new LinkedBlockingQueue();
      } else {
        this.responseQueue = responseQueue;
      }
    }

    // TODO WIP
    private fun OAuth2HttpResponse.toMockResponse(): MockResponse =
        MockResponse()
            .setHeaders(this.headers)
            .setResponseCode(this.status)
            .let {
      if (this.body != null) it.setBody(this.body) else it.setBody("")
    }

    @NotNull
    @Override
    public MockResponse dispatch(@NotNull RecordedRequest request) throws InterruptedException {
      if (responseQueue.peek() != null) {
        return responseQueue.take();
      } else {
        OAuth2HttpRequest foo = new OAuth2HttpRequest(request.getHeaders(), Objects.requireNonNull(request.getMethod()), Objects.requireNonNull(request.getRequestUrl()), request.getBody().copy().readUtf8());
        HttpParser.RequestHandler.
        return requestHandler.. .invoke(foo).toMockResponse();
      }
    }
  }
}

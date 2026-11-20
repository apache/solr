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
package org.apache.solr.handler.admin;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.SSLTestConfig;

/** Tests socket deadlines independently of a running ZooKeeper ensemble. */
public class ZookeeperStatusHandlerSocketTest extends SolrTestCase {
  private static final int SOCKET_TIMEOUT_MS = 1000;
  private static final int REQUEST_DEADLINE_MS = SOCKET_TIMEOUT_MS * 10;

  public void testPlaintextReadTimeoutClosesSocket() throws Exception {
    assertStalledServerTimesOut(false);
  }

  public void testTlsHandshakeTimeoutClosesSocket() throws Exception {
    assertStalledServerTimesOut(true);
  }

  public void testTlsDnsHostnameAndSni() throws Exception {
    SSLTestConfig ssl = configureTls(true);
    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("zk-status-sni"));
    try (SSLServerSocket server = newTlsServer(ssl);
        ZookeeperStatusHandler handler = new ZookeeperStatusHandler(null)) {
      var parameters = server.getSSLParameters();
      parameters.setSNIMatchers(
          List.of(
              new SNIMatcher(0) {
                @Override
                public boolean matches(SNIServerName name) {
                  return new SNIHostName("localhost").equals(name);
                }
              }));
      server.setSSLParameters(parameters);
      server.setSoTimeout(REQUEST_DEADLINE_MS);
      Future<Collection<SNIServerName>> request =
          executor.submit(
              () -> {
                try (SSLSocket peer = (SSLSocket) server.accept()) {
                  peer.setSoTimeout(REQUEST_DEADLINE_MS);
                  peer.startHandshake();
                  var names = ((ExtendedSSLSession) peer.getSession()).getRequestedServerNames();
                  assertEquals(
                      "ruok",
                      new String(peer.getInputStream().readNBytes(4), StandardCharsets.UTF_8));
                  peer.getOutputStream().write("imok\n".getBytes(StandardCharsets.UTF_8));
                  return names;
                }
              });
      assertEquals(
          List.of("imok"), handler.getZkRawResponse("localhost:" + server.getLocalPort(), "ruok"));
      assertEquals(
          List.of(new SNIHostName("localhost")),
          request.get(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
    }
  }

  public void testTlsRejectsMismatchedHostname() throws Exception {
    assertTlsHostnameVerification(true);
  }

  public void testTlsAllowsDisablingHostnameVerification() throws Exception {
    assertTlsHostnameVerification(false);
  }

  private void assertTlsHostnameVerification(boolean verifyHostname) throws Exception {
    for (boolean fips : List.of(false, true)) {
      SSLTestConfig ssl = configureTls(false);
      System.setProperty("zookeeper.fips-mode", Boolean.toString(fips));
      System.setProperty("zookeeper.ssl.hostnameVerification", Boolean.toString(verifyHostname));
      ExecutorService executor =
          ExecutorUtil.newMDCAwareSingleThreadExecutor(
              new SolrNamedThreadFactory("zk-status-hostname"));
      try (SSLServerSocket server = newTlsServer(ssl);
          ZookeeperStatusHandler handler = new ZookeeperStatusHandler(null)) {
        server.setSoTimeout(REQUEST_DEADLINE_MS);
        Future<String> command =
            executor.submit(
                () -> {
                  try (SSLSocket peer = (SSLSocket) server.accept()) {
                    peer.setSoTimeout(REQUEST_DEADLINE_MS);
                    peer.startHandshake();
                    String request =
                        new String(peer.getInputStream().readNBytes(4), StandardCharsets.UTF_8);
                    peer.getOutputStream().write("imok\n".getBytes(StandardCharsets.UTF_8));
                    return request;
                  }
                });
        String address = "localhost:" + server.getLocalPort();
        if (verifyHostname) {
          SolrException failure =
              expectThrows(SolrException.class, () -> handler.getZkRawResponse(address, "ruok"));
          assertTrue(failure.toString(), failure.getCause() instanceof SSLHandshakeException);
          expectThrows(
              ExecutionException.class,
              () -> command.get(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
        } else {
          assertEquals(List.of("imok"), handler.getZkRawResponse(address, "ruok"));
          assertEquals("ruok", command.get(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
        }
      } finally {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
      }
    }
  }

  /** A loopback TLS server socket on an ephemeral port, using {@code ssl}'s server keystore. */
  private static SSLServerSocket newTlsServer(SSLTestConfig ssl) throws Exception {
    return (SSLServerSocket)
        ssl.buildServerSSLConfig()
            .createContextFactory()
            .getSslContext()
            .getServerSocketFactory()
            .createServerSocket(0, 1, InetAddress.getLoopbackAddress());
  }

  private SSLTestConfig configureTls(boolean matchingHostname) throws Exception {
    System.setProperty("zookeeper.client.secure", "true");
    System.setProperty("zookeeper.fips-mode", Boolean.toString(random().nextBoolean()));
    SSLTestConfig ssl = new SSLTestConfig(true, false, matchingHostname);
    KeyStore certificates = KeyStore.getInstance("JKS");
    certificates.load(null, ssl.defaultKeyStorePassword().toCharArray());
    KeyStore keys = ssl.defaultKeyStore();
    for (String alias : Collections.list(keys.aliases())) {
      certificates.setCertificateEntry(alias, keys.getCertificate(alias));
    }
    Path trustStore = createTempDir().resolve("truststore.jks");
    try (var output = Files.newOutputStream(trustStore)) {
      certificates.store(output, ssl.defaultKeyStorePassword().toCharArray());
    }
    System.setProperty("zookeeper.ssl.trustStore.location", trustStore.toString());
    System.setProperty("zookeeper.ssl.trustStore.password", ssl.defaultKeyStorePassword());
    System.setProperty("zookeeper.ssl.trustStore.type", "JKS");
    return ssl;
  }

  private void assertStalledServerTimesOut(boolean secure) throws Exception {
    System.setProperty("zookeeper.client.secure", Boolean.toString(secure));
    System.setProperty("solr.zookeeper.client.timeout", Integer.toString(SOCKET_TIMEOUT_MS));
    System.setProperty(
        "solr.zookeeper.client.connect.timeout", Integer.toString(SOCKET_TIMEOUT_MS));
    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("zk-status-timeout"));
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        ZookeeperStatusHandler handler = new ZookeeperStatusHandler(null)) {
      server.setSoTimeout(REQUEST_DEADLINE_MS);
      Future<List<String>> response =
          executor.submit(
              () -> handler.getZkRawResponse("localhost:" + server.getLocalPort(), "ruok"));
      try (Socket peer = server.accept()) {
        peer.setSoTimeout(REQUEST_DEADLINE_MS);
        assertTrue(
            "Client must send a command or TLS handshake", peer.getInputStream().read() >= 0);
        ExecutionException failure =
            expectThrows(
                ExecutionException.class,
                () -> response.get(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
        assertTrue(failure.getCause().toString(), failure.getCause() instanceof SolrException);
        assertTrue(
            failure.getCause().toString(),
            failure.getCause().getCause() instanceof SocketTimeoutException);
        // Reading to EOF also verifies that timing out releases the connection.
        peer.getInputStream().readAllBytes();
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
    }
  }
}

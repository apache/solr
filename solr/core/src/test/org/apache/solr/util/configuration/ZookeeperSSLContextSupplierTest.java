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
package org.apache.solr.util.configuration;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.SSLTestConfig;
import org.apache.zookeeper.common.ClientX509Util;

/** Tests ZooKeeper TLS with the private-key truststores supported by Solr HTTPS. */
public class ZookeeperSSLContextSupplierTest extends SolrTestCase {
  private static final int HANDSHAKE_TIMEOUT_MS = 10000;
  private static final int REQUEST_DEADLINE_MS = HANDSHAKE_TIMEOUT_MS * 2;

  public void testPrivateKeyTrustStoreHandshake() throws Exception {
    for (boolean fips : List.of(false, true)) {
      configureTls(true, fips);
      assertHandshake(true);
    }
  }

  public void testPasswordPathsOverridePasswords() throws Exception {
    configureTls(true, random().nextBoolean());
    Path password = createTempDir().resolve("password");
    Files.writeString(password, new SSLTestConfig(true, false).defaultKeyStorePassword());
    System.setProperty("zookeeper.ssl.keyStore.passwordPath", password.toString());
    System.setProperty("zookeeper.ssl.trustStore.passwordPath", password.toString());
    System.setProperty("zookeeper.ssl.keyStore.password", "incorrect");
    System.setProperty("zookeeper.ssl.trustStore.password", "incorrect");
    assertHandshake(true);
  }

  public void testRevocationOptionsAcceptPrivateKeyTrustStore() throws Exception {
    configureTls(true, random().nextBoolean());
    for (String option : List.of("crl", "ocsp")) {
      System.setProperty("zookeeper.ssl." + option, "true");
      assertHandshake(true);
      System.setProperty("zookeeper.ssl." + option, "false");
    }
  }

  public void testHostnameVerificationRejectsMismatch() throws Exception {
    for (boolean fips : List.of(false, true)) {
      configureTls(false, fips);
      assertHandshake(false);
    }
  }

  public void testHostnameVerificationCanBeDisabled() throws Exception {
    for (boolean fips : List.of(false, true)) {
      configureTls(false, fips);
      System.setProperty("zookeeper.ssl.hostnameVerification", "false");
      assertHandshake(true);
    }
  }

  public void testUntrustedCertificateIsRejected() throws Exception {
    SSLContext untrustedServer =
        new SSLTestConfig(true, false, false)
            .buildServerSSLConfig()
            .createContextFactory()
            .getSslContext();
    for (boolean fips : List.of(false, true)) {
      configureTls(true, fips);
      System.setProperty("zookeeper.ssl.hostnameVerification", "false");
      assertHandshake(false, untrustedServer);
    }
  }

  private void configureTls(boolean matchingHostname, boolean fips) throws Exception {
    SSLTestConfig ssl = new SSLTestConfig(true, false, matchingHostname);
    var store = ssl.defaultKeyStore();
    for (String alias : Collections.list(store.aliases())) {
      assertTrue(store.isKeyEntry(alias));
      assertFalse(store.isCertificateEntry(alias));
    }
    Path storePath = createTempDir().resolve("store.jks");
    try (var output = Files.newOutputStream(storePath)) {
      store.store(output, ssl.defaultKeyStorePassword().toCharArray());
    }
    for (String name : List.of("keyStore", "trustStore")) {
      System.setProperty("zookeeper.ssl." + name + ".location", storePath.toString());
      System.setProperty("zookeeper.ssl." + name + ".type", "JKS");
      System.setProperty("zookeeper.ssl." + name + ".password", ssl.defaultKeyStorePassword());
    }
    System.setProperty("zookeeper.fips-mode", Boolean.toString(fips));
    System.setProperty("zookeeper.ssl.clientAuth", "need");
    System.setProperty("zookeeper.ssl.hostnameVerification", "true");
    System.setProperty(
        "zookeeper.ssl.context.supplier.class", ZookeeperSSLContextSupplier.class.getName());
  }

  private void assertHandshake(boolean succeeds) throws Exception {
    assertHandshake(succeeds, null);
  }

  private void assertHandshake(boolean succeeds, SSLContext serverContext) throws Exception {
    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("zk-ssl-context"));
    try (ClientX509Util x509 = new ClientX509Util();
        SSLServerSocket server =
            serverContext == null
                ? x509.createSSLServerSocket(0)
                : (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket(0);
        SSLSocket client = x509.createSSLSocket()) {
      server.setSoTimeout(REQUEST_DEADLINE_MS);
      Future<Integer> request =
          executor.submit(
              () -> {
                try (SSLSocket peer = (SSLSocket) server.accept()) {
                  peer.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
                  peer.startHandshake();
                  int value = peer.getInputStream().read();
                  peer.getOutputStream().write(value);
                  return value;
                }
              });
      client.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
      client.connect(
          new InetSocketAddress("localhost", server.getLocalPort()), HANDSHAKE_TIMEOUT_MS);
      if (succeeds) {
        client.startHandshake();
        client.getOutputStream().write(42);
        assertEquals(42, client.getInputStream().read());
        client.shutdownOutput();
        assertEquals(Integer.valueOf(42), request.get(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
      } else {
        expectThrows(SSLHandshakeException.class, client::startHandshake);
        expectThrows(
            ExecutionException.class,
            () -> request.get(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(REQUEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
    }
  }
}

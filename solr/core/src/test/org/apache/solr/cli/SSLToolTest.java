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

package org.apache.solr.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.EnvUtils;
import org.junit.Test;

/** Tests certificate conversion and local HTTPS configuration. */
public class SSLToolTest extends SolrTestCase {
  private Path prepare() throws Exception {
    assumeTrue(
        "External programs require -Ptests.useSecurityManager=false",
        EnvUtils.getProperty("java.security.manager") == null);
    assumeFalse(
        "solr.in.sh requires POSIX file permissions",
        EnvUtils.getProperty("os.name").startsWith("Windows"));
    Path dir = createTempDir();
    Files.writeString(
        dir.resolve("openssl.cnf"),
        "[req]\ndistinguished_name=dn\n[dn]\n[v3]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n");
    try {
      Process process =
          new ProcessBuilder(
                  "openssl",
                  "req",
                  "-x509",
                  "-newkey",
                  "rsa:2048",
                  "-nodes",
                  "-keyout",
                  dir.resolve("key.pem").toString(),
                  "-out",
                  dir.resolve("cert.pem").toString(),
                  "-days",
                  "2",
                  "-subj",
                  "/CN=localhost",
                  "-config",
                  dir.resolve("openssl.cnf").toString(),
                  "-extensions",
                  "v3")
              .redirectErrorStream(true)
              .redirectOutput(dir.resolve("openssl.log").toFile())
              .start();
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        process.destroyForcibly().waitFor();
        fail("Test certificate generation timed out");
      }
      assertEquals(Files.readString(dir.resolve("openssl.log")), 0, process.exitValue());
    } catch (IOException missingOpenSSL) {
      assumeNoException("OpenSSL must be installed", missingOpenSSL);
    }
    Files.writeString(dir.resolve("password.txt"), "Test'$(false)Password\n");
    Files.writeString(
        dir.resolve("solr.in.sh"),
        "# Keep this comment\nSOLR_HEAP=512m\nexport SOLR_SSL_ENABLED=false\nSOLR_SSL_KEY_STORE=old\n");
    return dir;
  }

  private SSLTool.SSLParams params(Path dir, String type, String zkHost) {
    return new SSLTool.SSLParams(
        dir.resolve("cert.pem"),
        dir.resolve("key.pem"),
        dir.resolve("password.txt"),
        null,
        dir.resolve("store"),
        type,
        dir.resolve("solr.in.sh"),
        zkHost);
  }

  @Test
  public void testBothFormats() throws Exception {
    for (String type : new String[] {"JKS", "PKCS12"}) {
      Path dir = prepare();
      var runtime = new CLITestHelper.TestingRuntime(true);
      String[] args = {
        "--cert",
        dir.resolve("cert.pem").toString(),
        "--key",
        dir.resolve("key.pem").toString(),
        "--password-file",
        dir.resolve("password.txt").toString(),
        "--keystore",
        dir.resolve("store").toString(),
        "--solr-include-file",
        dir.resolve("solr.in.sh").toString(),
        "--store-type",
        type
      };
      SSLTool tool = new SSLTool(runtime);
      assertEquals(0, new picocli.CommandLine(tool).execute(args));
      char[] password = "Test'$(false)Password".toCharArray();
      KeyStore store = KeyStore.getInstance(type);
      try (var input = Files.newInputStream(dir.resolve("store"))) {
        store.load(input, password);
      }
      assertNotNull(store.getKey("solr-ssl", password));
      assertEquals("RSA", store.getKey("solr-ssl", password).getAlgorithm());
      KeyStore trust = KeyStore.getInstance(type);
      try (var input = Files.newInputStream(dir.resolve("store.truststore"))) {
        trust.load(input, password);
      }
      assertTrue(trust.size() > 1);
      assertEquals(store.getCertificate("solr-ssl"), trust.getCertificate("solr-0"));
      assertFalse(runtime.getOutput().contains(new String(password)));
      String include = Files.readString(dir.resolve("solr.in.sh"));
      assertTrue(include.contains("SOLR_SSL_ENABLED='true'"));
      assertTrue(include.contains("SOLR_SSL_KEY_STORE_TYPE='" + type + "'"));
    }
  }

  @Test
  public void testSecureExampleImportsCertificate() throws Exception {
    Path dir = prepare();
    var runtime = new CLITestHelper.TestingRuntime(true);
    SecureExampleSSL ssl = new SecureExampleSSL(dir.resolve("example"), runtime);
    assertFalse(ssl.isConfigured());
    ssl.install(dir.resolve("cert.pem"), dir.resolve("key.pem"), dir.resolve("password.txt"), null);
    assertTrue(ssl.isConfigured());
    assertArrayEquals(
        Files.readAllBytes(dir.resolve("cert.pem")), Files.readAllBytes(ssl.certificate()));
    ssl.configureClientTrust();
    assertEquals(ssl.keystore() + ".truststore", EnvUtils.getProperty("javax.net.ssl.trustStore"));
    assertEquals("Test'$(false)Password", EnvUtils.getProperty("javax.net.ssl.trustStorePassword"));
    ssl.printVerificationInstructions("https://localhost:8983/solr");
    assertTrue(runtime.getOutput().contains("https://localhost:8983/api/node/system"));
    assertTrue(ssl.isSelfSigned());
    assertEquals(
        "# Keep this comment\nSOLR_HEAP=512m\nexport SOLR_SSL_ENABLED=false\nSOLR_SSL_KEY_STORE=old\n",
        Files.readString(dir.resolve("solr.in.sh")));
  }

  @Test
  public void testInvalidKeyLeavesConfigurationUntouched() throws Exception {
    Path dir = prepare();
    String original = Files.readString(dir.resolve("solr.in.sh"));
    Files.writeString(dir.resolve("key.pem"), "not a private key");
    expectThrows(
        IllegalArgumentException.class, () -> new SSLTool().configure(params(dir, "JKS", null)));
    assertEquals(original, Files.readString(dir.resolve("solr.in.sh")));
    assertFalse(Files.exists(dir.resolve("store")));
  }

  @Test
  public void testClusterPropertyAndRollback() throws Exception {
    Path dir = prepare();
    ZkTestServer server = new ZkTestServer(createTempDir());
    server.run();
    try {
      new SSLTool().configure(params(dir, "JKS", server.getZkAddress()));
      try (SolrZkClient client =
          new SolrZkClient.Builder().withUrl(server.getZkAddress()).build()) {
        var properties = new ClusterProperties(client).getClusterProperties();
        assertEquals("https", properties.get("urlScheme"));
      }
      byte[] originalStore = Files.readAllBytes(dir.resolve("store"));
      String originalInclude = Files.readString(dir.resolve("solr.in.sh"));
      expectThrows(
          IllegalArgumentException.class,
          () -> new SSLTool().configure(params(dir, "JKS", server.getZkAddress() + "/missing")));
      assertArrayEquals(originalStore, Files.readAllBytes(dir.resolve("store")));
      assertEquals(originalInclude, Files.readString(dir.resolve("solr.in.sh")));
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testWindowsUnsupportedMessage() {
    System.setProperty("os.name", "Windows 11");
    var error =
        expectThrows(UnsupportedOperationException.class, () -> new SSLTool().configure(null));
    assertTrue(error.getMessage().contains("not supported on Windows"));
    assertTrue(error.getMessage().contains("Configure HTTPS manually"));
  }
}

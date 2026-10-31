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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Manages the HTTPS files and certificate verification for the secure Solr example. */
final class SecureExampleSSL {
  private final Path directory;
  private final ToolRuntime runtime;

  SecureExampleSSL(Path solrHome, ToolRuntime runtime) {
    this.directory = solrHome.toAbsolutePath().resolve("tls");
    this.runtime = runtime;
  }

  Path includeFile() {
    return directory.resolve("solr.in.sh");
  }

  Path certificate() {
    return directory.resolve("cert.pem");
  }

  Path passwordFile() {
    return directory.resolve("keystore-password.txt");
  }

  Path keystore() {
    return directory.resolve("solr-ssl.keystore.jks");
  }

  boolean isConfigured() {
    return Files.isRegularFile(includeFile())
        && Files.isRegularFile(certificate())
        && Files.isRegularFile(passwordFile())
        && Files.isRegularFile(keystore())
        && Files.isRegularFile(Path.of(keystore() + ".truststore"));
  }

  void generate(String password) throws Exception {
    Files.createDirectories(directory);
    IncludeFileEditor.write(passwordFile(), password.getBytes(StandardCharsets.UTF_8));
    Path key = directory.resolve("key.pem");
    IncludeFileEditor.write(key, new byte[0]);
    Path config = directory.resolve("openssl.cnf");
    Files.writeString(
        config,
        "[req]\ndistinguished_name=dn\n[dn]\n[v3]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n");
    run(
        List.of(
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            key.toString(),
            "-out",
            certificate().toString(),
            "-days",
            "30",
            "-subj",
            "/CN=localhost",
            "-config",
            config.toString(),
            "-extensions",
            "v3"),
        directory.resolve("openssl.log"));
    install(certificate(), key, passwordFile(), null);
  }

  void install(Path cert, Path key, Path password, Path keyPassword) throws Exception {
    Files.createDirectories(directory);
    if (!Files.exists(includeFile())) IncludeFileEditor.write(includeFile(), new byte[0]);
    new SSLTool(runtime)
        .configure(
            new SSLTool.SSLParams(
                cert, key, password, keyPassword, keystore(), "JKS", includeFile(), null));
    IncludeFileEditor.write(certificate(), Files.readAllBytes(cert));
    IncludeFileEditor.write(passwordFile(), Files.readAllBytes(password));
    runtime.println("Embedded ZooKeeper will configure urlScheme=https when Solr starts.");
  }

  void configureClientTrust() throws IOException {
    System.setProperty("javax.net.ssl.trustStore", keystore() + ".truststore");
    System.setProperty("javax.net.ssl.trustStoreType", "JKS");
    System.setProperty(
        "javax.net.ssl.trustStorePassword",
        Files.readString(passwordFile()).replaceFirst("\\r?\\n$", ""));
  }

  void setAdvertisedHost(String host) throws IOException {
    IncludeFileEditor.write(
        includeFile(),
        IncludeFileEditor.update(
                Files.readString(includeFile()),
                Map.of("SOLR_HOST_ADVERTISE", host),
                Set.of("SOLR_HOST_ADVERTISE"),
                IncludeFileEditor.Format.SHELL)
            .getBytes(StandardCharsets.UTF_8));
  }

  void printFiles() {
    runtime.println("Certificate: " + certificate());
    Path key = directory.resolve("key.pem");
    if (Files.isRegularFile(key)) runtime.println("Private key: " + key);
    runtime.println("Keystore: " + keystore());
    runtime.println("Truststore: " + keystore() + ".truststore");
    runtime.println("Keystore password file: " + passwordFile());
    runtime.println("Solr include file: " + includeFile());
  }

  void printVerificationInstructions(String solrUrl) {
    String url = CLIUtils.normalizeSolrUrl(solrUrl, false) + "/api/node/system";
    runtime.println("Verify HTTPS in another terminal:");
    runtime.println("curl --cacert " + quote(certificate().toString()) + " " + quote(url));
    runtime.println("You should see Solr system information without a certificate error.");
    runtime.println(
        "If authentication is already enabled, an HTTP 401 response also confirms HTTPS.");
  }

  boolean isSelfSigned() throws IOException, GeneralSecurityException {
    try (var input = Files.newInputStream(certificate())) {
      var cert =
          (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
      if (!cert.getSubjectX500Principal().equals(cert.getIssuerX500Principal())) return false;
      try {
        cert.verify(cert.getPublicKey());
        return true;
      } catch (GeneralSecurityException e) {
        return false;
      }
    }
  }

  private static String quote(String value) {
    return "'" + value.replace("'", "'\"'\"'") + "'";
  }

  private static void run(List<String> command, Path output) throws Exception {
    Process process =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    process.getOutputStream().close();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly().waitFor();
      throw new IllegalStateException(command.getFirst() + " timed out.");
    }
    if (process.exitValue() != 0) {
      throw new IllegalStateException(
          command.getFirst() + " failed: " + Files.readString(output).trim());
    }
  }
}

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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.EnvUtils;
import picocli.CommandLine;

/** Installs PEM certificates and configures HTTPS for a Solr node. */
@CommandLine.Command(
    name = "ssl",
    description = "Convert PEM files to a Java keystore and configure HTTPS in solr.in.sh.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  bin/solr ssl --cert fullchain.pem --key privkey.pem --password-file password.txt",
      "  bin/solr ssl --cert fullchain.pem --key privkey.pem --password-file password.txt --zk-host localhost:9983"
    })
public class SSLTool extends ToolBase {
  @CommandLine.Option(
      names = "--cert",
      paramLabel = "FILE",
      required = true,
      description = "PEM certificate chain, with the server certificate first.")
  private Path certificate;

  @CommandLine.Option(
      names = "--key",
      paramLabel = "FILE",
      required = true,
      description = "PEM private key. Requires OpenSSL on PATH.")
  private Path privateKey;

  @CommandLine.Option(
      names = "--password-file",
      paramLabel = "FILE",
      required = true,
      description = "File containing the keystore password on one line.")
  private Path passwordFile;

  @CommandLine.Option(
      names = "--key-password-file",
      paramLabel = "FILE",
      description = "Password file for an encrypted PEM private key.")
  private Path keyPasswordFile;

  @CommandLine.Option(
      names = "--keystore",
      paramLabel = "FILE",
      description = "Destination keystore. Default directory: server/etc in the Solr installation.")
  private Path keystore;

  @CommandLine.Option(
      names = "--store-type",
      paramLabel = "TYPE",
      defaultValue = "JKS",
      description = "Keystore format: JKS or PKCS12. Default: JKS.")
  private String storeType = "JKS";

  @CommandLine.Option(
      names = "--solr-include-file",
      paramLabel = "FILE",
      description =
          "Existing solr.in.sh to update. Defaults to bin/solr.in.sh in the Solr installation.")
  private Path includeFile;

  @CommandLine.Option(
      names = "--zk-host",
      paramLabel = "HOST",
      description =
          "ZooKeeper connection string. If supplied, set urlScheme=https for the entire cluster.")
  private String zkHost;

  /** Parameters for the SSL command, independent of the command line parser. */
  record SSLParams(
      Path certificate,
      Path privateKey,
      Path passwordFile,
      Path keyPasswordFile,
      Path keystore,
      String storeType,
      Path includeFile,
      String zkHost) {}

  public SSLTool() {
    this(new DefaultToolRuntime());
  }

  public SSLTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "ssl";
  }

  @Override
  public int callTool() throws Exception {
    configure(
        new SSLParams(
            certificate,
            privateKey,
            passwordFile,
            keyPasswordFile,
            keystore,
            storeType,
            includeFile,
            zkHost));
    echo("Restart Solr to enable HTTPS.");
    if (zkHost == null) {
      echo(
          "No cluster property changed. For SolrCloud, set urlScheme=https with bin/solr cluster before restarting nodes.");
    }
    return 0;
  }

  void configure(SSLParams params) throws Exception {
    if (EnvUtils.getProperty("os.name", "").startsWith("Windows")) {
      throw new UnsupportedOperationException(
          "bin/solr ssl is not supported on Windows. Configure HTTPS manually using the Enabling SSL section of the Solr Reference Guide.");
    }
    String type = params.storeType().toUpperCase(Locale.ROOT);
    if (!type.equals("JKS") && !type.equals("PKCS12")) {
      throw new IllegalArgumentException("--store-type must be JKS or PKCS12.");
    }
    Path install = Path.of(EnvUtils.getProperty("solr.install.dir", ".")).toAbsolutePath();
    Path include =
        (params.includeFile() == null ? install.resolve("bin/solr.in.sh") : params.includeFile())
            .toRealPath();
    if (!include.getFileName().toString().endsWith(".sh") || !Files.isWritable(include)) {
      throw new IllegalArgumentException(
          "--solr-include-file must be a writable shell include file (.sh).");
    }
    Path target =
        (params.keystore() == null
                ? install.resolve(
                    "server/etc/solr-ssl.keystore." + (type.equals("JKS") ? "jks" : "p12"))
                : params.keystore())
            .toAbsolutePath()
            .normalize();
    Path trustTarget = target.resolveSibling(target.getFileName() + ".truststore");
    if (target.toString().chars().anyMatch(Character::isWhitespace)) {
      throw new IllegalArgumentException("The keystore path must not contain whitespace.");
    }
    for (Path output : new Path[] {target, trustTarget}) {
      if (Files.isSymbolicLink(output)
          || !Files.isDirectory(output.getParent())
          || !Files.isWritable(output.getParent())) {
        throw new IllegalArgumentException(
            "Keystore destinations must have writable existing parent directories and must not be symbolic links.");
      }
      for (Path input :
          new Path[] {params.certificate(), params.privateKey(), params.passwordFile(), include}) {
        if (output.equals(input.toAbsolutePath().normalize())
            || (Files.exists(output) && Files.isSameFile(output, input))) {
          throw new IllegalArgumentException("Keystore destinations must differ from input files.");
        }
      }
      if (params.keyPasswordFile() != null
          && output.equals(params.keyPasswordFile().toAbsolutePath().normalize())) {
        throw new IllegalArgumentException("Keystore destinations must differ from input files.");
      }
    }
    String password = readPassword(params.passwordFile());
    char[] secret = password.toCharArray();
    Path temporary = Files.createTempDirectory("solr-ssl-");
    try {
      Path converted = temporary.resolve("converted.p12");
      ProcessBuilder builder =
          new ProcessBuilder(
              "openssl",
              "pkcs12",
              "-export",
              "-in",
              params.certificate().toAbsolutePath().toString(),
              "-inkey",
              params.privateKey().toAbsolutePath().toString(),
              "-name",
              "solr-ssl",
              "-out",
              converted.toString(),
              "-passout",
              "env:SOLR_SSL_CONVERSION_PASSWORD",
              "-passin",
              "env:SOLR_SSL_PEM_PASSWORD");
      builder.environment().put("SOLR_SSL_CONVERSION_PASSWORD", password);
      builder
          .environment()
          .put(
              "SOLR_SSL_PEM_PASSWORD",
              params.keyPasswordFile() == null ? "" : readPassword(params.keyPasswordFile()));
      builder.redirectErrorStream(true).redirectOutput(temporary.resolve("openssl.log").toFile());
      Process process = builder.start();
      process.getOutputStream().close();
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        process.destroyForcibly().waitFor();
        throw new IllegalStateException("OpenSSL conversion timed out.");
      }
      if (process.exitValue() != 0) {
        throw new IllegalArgumentException(
            "OpenSSL could not convert the PEM files. Check the certificate, matching private key, and key password.");
      }
      KeyStore source = KeyStore.getInstance("PKCS12");
      try (var input = Files.newInputStream(converted)) {
        source.load(input, secret);
      }
      var chain = source.getCertificateChain("solr-ssl");
      for (var cert : chain) {
        ((X509Certificate) cert).checkValidity();
      }
      KeyStore store = KeyStore.getInstance(type);
      store.load(null, secret);
      store.setKeyEntry("solr-ssl", source.getKey("solr-ssl", secret), secret, chain);
      KeyStore trust = KeyStore.getInstance(type);
      trust.load(null, secret);
      Path defaultTrust = Path.of(EnvUtils.getProperty("java.home"), "lib/security/cacerts");
      KeyStore defaults = KeyStore.getInstance(defaultTrust.toFile(), (char[]) null);
      var aliases = defaults.aliases();
      while (aliases.hasMoreElements()) {
        String alias = aliases.nextElement();
        trust.setCertificateEntry("jdk-" + alias, defaults.getCertificate(alias));
      }
      for (int i = 0; i < chain.length; i++) {
        trust.setCertificateEntry("solr-" + i, chain[i]);
      }
      String original = Files.readString(include);
      Map<String, String> settings = new LinkedHashMap<>();
      settings.put("SOLR_SSL_ENABLED", "true");
      settings.put("SOLR_SSL_KEY_STORE", target.toString());
      settings.put("SOLR_SSL_KEY_STORE_PASSWORD", password);
      settings.put("SOLR_SSL_KEY_STORE_TYPE", type);
      settings.put("SOLR_SSL_TRUST_STORE", trustTarget.toString());
      settings.put("SOLR_SSL_TRUST_STORE_PASSWORD", password);
      settings.put("SOLR_SSL_TRUST_STORE_TYPE", type);
      settings.put("SOLR_SSL_NEED_CLIENT_AUTH", "false");
      settings.put("SOLR_SSL_WANT_CLIENT_AUTH", "false");
      settings.put("SOLR_SSL_CHECK_PEER_NAME", "true");
      Map<Path, byte[]> files = new LinkedHashMap<>();
      files.put(target, serialize(store, secret));
      files.put(trustTarget, serialize(trust, secret));
      files.put(
          include,
          IncludeFileEditor.update(
                  original, settings, settings.keySet(), IncludeFileEditor.Format.SHELL)
              .getBytes(StandardCharsets.UTF_8));
      Map<Path, byte[]> previous = new LinkedHashMap<>();
      for (Path file : files.keySet()) {
        previous.put(file, Files.exists(file) ? Files.readAllBytes(file) : null);
      }
      try {
        for (var file : files.entrySet()) {
          IncludeFileEditor.write(file.getKey(), file.getValue());
        }
        if (params.zkHost() != null) {
          int slash = params.zkHost().indexOf('/');
          if (slash >= 0) {
            try (SolrZkClient rootClient =
                new SolrZkClient.Builder()
                    .withUrl(params.zkHost().substring(0, slash))
                    .withConnTimeOut(3, TimeUnit.SECONDS)
                    .build()) {
              if (!rootClient.exists(params.zkHost().substring(slash))) {
                throw new IllegalArgumentException("The ZooKeeper chroot does not exist.");
              }
            }
          }
          try (SolrZkClient client =
              new SolrZkClient.Builder()
                  .withUrl(params.zkHost())
                  .withConnTimeOut(3, TimeUnit.SECONDS)
                  .build()) {
            if (!client.exists("/")) {
              throw new IllegalArgumentException("The ZooKeeper chroot does not exist.");
            }
            new ClusterProperties(client).setClusterProperty("urlScheme", "https");
          }
          echo("Set cluster property urlScheme=https.");
        }
      } catch (Exception failure) {
        for (var file : previous.entrySet()) {
          try {
            if (file.getValue() == null) Files.deleteIfExists(file.getKey());
            else IncludeFileEditor.write(file.getKey(), file.getValue());
          } catch (Exception rollback) {
            failure.addSuppressed(rollback);
          }
        }
        throw failure;
      }
      echo("Installed " + type + " keystore: " + target);
      echo("Updated " + include + ".");
    } finally {
      Arrays.fill(secret, '\0');
      try (var files = Files.list(temporary)) {
        for (Path file : files.toList()) Files.deleteIfExists(file);
      }
      Files.deleteIfExists(temporary);
    }
  }

  private static String readPassword(Path path) throws Exception {
    String value = Files.readString(path).replaceFirst("\\r?\\n$", "");
    if (value.length() < 6
        || value.indexOf('\n') >= 0
        || value.indexOf('\r') >= 0
        || value.indexOf('\0') >= 0) {
      throw new IllegalArgumentException(
          "Password files must contain one line of at least six characters.");
    }
    return value;
  }

  private static byte[] serialize(KeyStore store, char[] password) throws Exception {
    var output = new ByteArrayOutputStream();
    store.store(output, password);
    return output.toByteArray();
  }
}

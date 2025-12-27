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
package org.apache.solr.bootstrap;

import java.io.FileOutputStream;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.solr.common.util.EnvUtils;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates self-signed SSL certificates for development and testing purposes. This class provides
 * automatic certificate generation triggered by the SOLR_SSL_GENERATE environment variable.
 *
 * <p>This is intended for development, testing, and local deployments only. Production deployments
 * should use proper CA-signed certificates from Let's Encrypt or similar certificate authorities.
 *
 * @since Solr 11.0 (SIP-6 Phase 1)
 */
public class SslCertificateGenerator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Container for SSL configuration information. */
  public static class SslConfiguration {
    private final boolean hasKeyStore;
    private final boolean hasTrustStore;
    private final Path generatedKeystorePath; // null if using provided keystore
    private final String generatedPassword; // null if using provided keystore

    public SslConfiguration(
        boolean hasKeyStore,
        boolean hasTrustStore,
        Path generatedKeystorePath,
        String generatedPassword) {
      this.hasKeyStore = hasKeyStore;
      this.hasTrustStore = hasTrustStore;
      this.generatedKeystorePath = generatedKeystorePath;
      this.generatedPassword = generatedPassword;
    }

    public boolean hasKeyStore() {
      return hasKeyStore;
    }

    public boolean hasTrustStore() {
      return hasTrustStore;
    }

    public Path getGeneratedKeystorePath() {
      return generatedKeystorePath;
    }

    public String getGeneratedPassword() {
      return generatedPassword;
    }

    public boolean isGenerated() {
      return generatedKeystorePath != null;
    }
  }

  /**
   * Configure SSL based on environment variables and user preferences. This method implements the
   * following decision logic:
   *
   * <ol>
   *   <li>Check if user provided keystores (SOLR_SSL_KEY_STORE or SOLR_SSL_TRUST_STORE)
   *   <li>Auto-enable SSL if keystores are present (backward compatibility)
   *   <li>If SSL not enabled, return null (HTTP mode)
   *   <li>If SSL enabled but no keystores, check SOLR_SSL_GENERATE (defaults to true)
   *   <li>Generate self-signed certificate if SOLR_SSL_GENERATE=true
   *   <li>Error if SSL enabled but no keystores and SOLR_SSL_GENERATE=false
   * </ol>
   *
   * @return SslConfiguration with SSL settings, or null if SSL is disabled (HTTP mode)
   * @throws IllegalStateException if SSL enabled but no keystores and generation disabled
   * @throws Exception if certificate generation fails
   */
  public static SslConfiguration configureSsl() throws Exception {
    // 1. Check if user provided keystores
    boolean hasKeyStore = EnvUtils.getProperty("solr.ssl.key.store") != null;
    boolean hasTrustStore = EnvUtils.getProperty("solr.ssl.trust.store") != null;
    boolean hasAnyStore = hasKeyStore || hasTrustStore;

    // 2. Get SOLR_SSL_ENABLED (defaults to auto-enable if keystores present)
    Boolean sslEnabledProp = EnvUtils.getPropertyAsBool("solr.ssl.enabled");
    boolean sslEnabled = (sslEnabledProp != null) ? sslEnabledProp : hasAnyStore;

    // 3. If SSL not enabled, return null (HTTP mode)
    if (!sslEnabled) {
      log.info("SSL disabled (HTTP mode)");
      return null;
    }

    // 4. If SSL enabled but no keystores, check SOLR_SSL_GENERATE (defaults to true)
    if (!hasAnyStore) {
      boolean sslGenerate = EnvUtils.getPropertyAsBool("solr.ssl.generate", true);

      if (!sslGenerate) {
        throw new IllegalStateException(
            "SSL enabled but no keystores configured and SOLR_SSL_GENERATE=false. "
                + "Either set SOLR_SSL_KEY_STORE/SOLR_SSL_TRUST_STORE or enable SOLR_SSL_GENERATE.");
      }

      // Generate self-signed certificate
      log.info("SSL enabled with auto-generation");
      return generateSelfSignedCertificate();
    }

    // 5. Use provided keystores
    log.info(
        "SSL enabled with user-provided keystores (keyStore={}, trustStore={})",
        hasKeyStore,
        hasTrustStore);
    return new SslConfiguration(hasKeyStore, hasTrustStore, null, null);
  }

  /**
   * Generate a self-signed certificate for development/testing.
   *
   * @return SslConfiguration with generated certificate path and password
   * @throws Exception if certificate generation fails
   */
  private static SslConfiguration generateSelfSignedCertificate() throws Exception {
    Path keystorePath = determineKeystorePath();

    // Skip if already exists
    if (Files.exists(keystorePath)) {
      log.info("Keystore already exists at {}, reusing", keystorePath);
      // Load password from file and return existing cert info
      Path passwordPath = Path.of(keystorePath.toString() + ".password");
      if (Files.exists(passwordPath)) {
        String password = Files.readString(passwordPath).trim();
        return new SslConfiguration(true, true, keystorePath, password);
      }
      throw new IllegalStateException(
          "Keystore exists at "
              + keystorePath
              + " but password file is missing. "
              + "Delete the keystore or provide the password file.");
    }

    log.info("╔══════════════════════════════════════════════════════════════╗");
    log.info("║  Generating self-signed SSL certificate                      ║");
    log.info("╚══════════════════════════════════════════════════════════════╝");

    String password = generateSecurePassword();
    List<GeneralName> sans = parseSans();

    KeyStore keystore = createKeystore(password, sans);
    saveKeystore(keystore, keystorePath, password);
    savePassword(keystorePath, password);

    log.info("✓ Generated certificate at {}", keystorePath);
    log.info("✓ Password saved to {}.password", keystorePath);

    return new SslConfiguration(true, true, keystorePath, password);
  }

  /**
   * Determine keystore path from environment variables. Uses SOLR_SSL_KEY_STORE if set, otherwise
   * defaults to $SOLR_PID_DIR/solr-ssl.keystore.p12.
   */
  private static Path determineKeystorePath() {
    // SOLR_SSL_KEY_STORE env var → solr.ssl.key.store system property
    String envPath = EnvUtils.getProperty("solr.ssl.key.store");
    if (envPath != null && !envPath.isEmpty()) {
      return Path.of(envPath);
    }
    // solr.pid.dir is set by bin/solr script
    // If not set, default to ${solr.install.dir}/bin
    String solrPidDir = EnvUtils.getProperty("solr.pid.dir");
    if (solrPidDir == null || solrPidDir.isEmpty()) {
      String installDir = EnvUtils.getProperty("solr.install.dir", ".");
      solrPidDir = installDir + "/bin";
    }
    return Path.of(solrPidDir, "solr-ssl.keystore.p12");
  }

  /**
   * Create a self-signed certificate keystore using BouncyCastle.
   *
   * @param password keystore password
   * @param sans list of Subject Alternative Names
   * @return KeyStore containing the generated certificate
   */
  private static KeyStore createKeystore(String password, List<GeneralName> sans) throws Exception {
    // 1. Generate RSA 2048-bit key pair
    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(2048);
    KeyPair keyPair = kpg.generateKeyPair();

    // 2. Add BouncyCastle provider
    Provider bc = new BouncyCastleProvider();
    Security.addProvider(bc);

    // 3. Create certificate builder
    X500Name subject = new X500Name("CN=localhost, OU=Solr Bootstrap, O=Apache Solr");
    X500Name issuer = subject; // Self-signed

    Instant now = Instant.now();
    Instant expiry = now.plus(365, ChronoUnit.DAYS);
    BigInteger serial = BigInteger.valueOf(now.toEpochMilli());

    JcaX509v3CertificateBuilder certBuilder =
        new JcaX509v3CertificateBuilder(
            issuer, serial, Date.from(now), Date.from(expiry), subject, keyPair.getPublic());

    // 4. Add SAN extension
    GeneralName[] sanArray = sans.toArray(new GeneralName[0]);
    certBuilder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(sanArray));

    // 5. Sign certificate
    ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA").build(keyPair.getPrivate());
    X509Certificate cert =
        new JcaX509CertificateConverter().setProvider(bc).getCertificate(certBuilder.build(signer));

    // 6. Create PKCS12 keystore
    KeyStore ks = KeyStore.getInstance("PKCS12");
    ks.load(null, null);
    ks.setKeyEntry("solr", keyPair.getPrivate(), password.toCharArray(), new Certificate[] {cert});

    return ks;
  }

  /**
   * Parse Subject Alternative Names from SOLR_SSL_CERT_SAN environment variable. Format:
   * DNS:hostname,IP:address Default: IP:127.0.0.1
   */
  private static List<GeneralName> parseSans() {
    // SOLR_SSL_CERT_SAN env var → solr.ssl.cert.san system property
    String sansStr = EnvUtils.getProperty("solr.ssl.cert.san", "IP:127.0.0.1");
    List<GeneralName> sans = new ArrayList<>();

    for (String san : sansStr.split(",")) {
      san = san.trim();
      if (san.startsWith("DNS:")) {
        sans.add(new GeneralName(GeneralName.dNSName, san.substring(4)));
      } else if (san.startsWith("IP:")) {
        sans.add(new GeneralName(GeneralName.iPAddress, san.substring(3)));
      } else {
        log.warn("Invalid SAN format: {}. Use DNS:hostname or IP:address", san);
      }
    }

    return sans;
  }

  /** Generate a secure random 32-character alphanumeric password. */
  private static String generateSecurePassword() {
    SecureRandom random = new SecureRandom();
    String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder(32);
    for (int i = 0; i < 32; i++) {
      sb.append(chars.charAt(random.nextInt(chars.length())));
    }
    return sb.toString();
  }

  /** Save keystore to file. */
  private static void saveKeystore(KeyStore ks, Path keystorePath, String password)
      throws Exception {
    Files.createDirectories(keystorePath.getParent());
    try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
      ks.store(fos, password.toCharArray());
    }
  }

  /**
   * Save password to <keystorePath>.password file with restricted permissions. Sets 600 (owner-only
   * read/write) on POSIX systems.
   */
  private static void savePassword(Path keystorePath, String password) throws Exception {
    Path passwordPath = Path.of(keystorePath.toString() + ".password");
    Files.writeString(passwordPath, password);

    // Set 600 permissions (owner read/write only)
    try {
      Files.setPosixFilePermissions(passwordPath, PosixFilePermissions.fromString("rw-------"));
      log.info("✓ Set password file permissions to 600");
    } catch (UnsupportedOperationException e) {
      log.warn("Cannot set POSIX permissions (Windows?). Password file may be readable by others.");
    }
  }
}

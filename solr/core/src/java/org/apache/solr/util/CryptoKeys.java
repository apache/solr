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
package org.apache.solr.util;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A utility class with helpers for various signature and certificate tasks */
public final class CryptoKeys {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String CIPHER_ALGORITHM = "RSA/ECB/nopadding";

  private final Map<String, PublicKey> keys;
  private Exception exception;

  public CryptoKeys(Map<String, byte[]> trustedKeys) throws Exception {
    HashMap<String, PublicKey> m = new HashMap<>();
    for (Map.Entry<String, byte[]> e : trustedKeys.entrySet()) {
      m.put(e.getKey(), getX509PublicKey(e.getValue()));
    }
    this.keys = Map.copyOf(m);
  }

  /** Try with all signatures and return the name of the signature that matched */
  public String verify(String sig, ByteBuffer data) {
    exception = null;
    for (Map.Entry<String, PublicKey> entry : keys.entrySet()) {
      boolean verified;
      try {
        verified = CryptoKeys.verify(entry.getValue(), Base64.getDecoder().decode(sig), data);
        log.debug("verified {} ", verified);
        if (verified) return entry.getKey();
      } catch (Exception e) {
        exception = e;
        log.debug("NOT verified  ");
      }
    }

    return null;
  }

  public String verify(String sig, InputStream is) {
    exception = null;
    for (Map.Entry<String, PublicKey> entry : keys.entrySet()) {
      boolean verified;
      try {
        verified = CryptoKeys.verify(entry.getValue(), Base64.getDecoder().decode(sig), is);
        log.debug("verified {} ", verified);
        if (verified) return entry.getKey();
      } catch (Exception e) {
        exception = e;
        log.debug("NOT verified  ");
      }
    }

    return null;
  }

  /** Create PublicKey from a .DER file */
  public static PublicKey getX509PublicKey(byte[] buf) throws InvalidKeySpecException {
    X509EncodedKeySpec spec = new X509EncodedKeySpec(buf);
    try {
      KeyFactory kf = KeyFactory.getInstance("RSA");
      return kf.generatePublic(spec);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("JVM spec is required to support RSA", e);
    }
  }

  /**
   * Verify the signature of a file
   *
   * @param publicKey the public key used to sign this
   * @param sig the signature
   * @param data The data tha is signed
   */
  public static boolean verify(PublicKey publicKey, byte[] sig, ByteBuffer data)
      throws InvalidKeyException, SignatureException {
    data = ByteBuffer.wrap(data.array(), data.arrayOffset(), data.limit());
    try {
      Signature signature = Signature.getInstance("SHA1withRSA");
      signature.initVerify(publicKey);
      signature.update(data);
      return signature.verify(sig);
    } catch (NoSuchAlgorithmException e) {
      // wil not happen
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static boolean verify(PublicKey publicKey, byte[] sig, InputStream is)
      throws InvalidKeyException, SignatureException, IOException {
    try {
      Signature signature = Signature.getInstance("SHA1withRSA");
      signature.initVerify(publicKey);
      byte[] buf = new byte[1024];
      while (true) {
        int sz = is.read(buf);
        if (sz == -1) break;
        signature.update(buf, 0, sz);
      }
      try {
        return signature.verify(sig);
      } catch (SignatureException e) {
        return false;
      }
    } catch (NoSuchAlgorithmException e) {
      // will not happen
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static PublicKey deserializeX509PublicKey(String pubKey) {
    try {
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(pubKey));
      return keyFactory.generatePublic(publicKeySpec);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static byte[] decryptRSA(byte[] buffer, PublicKey pubKey)
      throws InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
    Cipher rsaCipher;
    try {
      rsaCipher = Cipher.getInstance(CIPHER_ALGORITHM);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    rsaCipher.init(Cipher.DECRYPT_MODE, pubKey);
    return rsaCipher.doFinal(buffer);
  }

  public static boolean verifySha256(byte[] data, byte[] sig, PublicKey key)
      throws SignatureException, InvalidKeyException {
    try {
      Signature signature = Signature.getInstance("SHA256withRSA");
      signature.initVerify(key);
      signature.update(data);
      return signature.verify(sig);
    } catch (NoSuchAlgorithmException e) {
      throw new InternalError("SHA256withRSA must be supported by the JVM.");
    }
  }

  /**
   * Tries for find X509 certificates in the input stream in DER or PEM format. Supports multiple
   * certs in same stream if multiple PEM certs are concatenated.
   *
   * @param certsStream input stream with the contents of either PEM (plaintext) or DER (binary)
   *     certs
   * @return collection of found certificates, else throws exception
   */
  public static Collection<X509Certificate> parseX509Certs(InputStream certsStream) {
    try {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Collection<? extends Certificate> parsedCerts = cf.generateCertificates(certsStream);
      List<X509Certificate> certs =
          parsedCerts.stream()
              .filter(c -> c instanceof X509Certificate)
              .map(c -> (X509Certificate) c)
              .collect(Collectors.toList());
      if (certs.size() > 0) {
        return certs;
      } else {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Wrong type of certificates. Must be DER or PEM format");
      }
    } catch (CertificateException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed loading certificate(s) from input stream",
          e);
    }
  }

  /**
   * Given a file, will try to
   *
   * @param pemContents the raw string content of the PEM file
   * @return the certificate content between BEGIN and END markers
   */
  public static String extractCertificateFromPem(String pemContents) {
    int from = pemContents.indexOf("-----BEGIN CERTIFICATE-----");
    int end = pemContents.lastIndexOf("-----END CERTIFICATE-----") + 25;
    return pemContents.substring(from, end);
  }

  public static class RSAKeyPair {
    private final String pubKeyStr;
    private final PublicKey publicKey;
    private final PrivateKey privateKey;

    // If this ever comes back to haunt us see the discussion at
    // SOLR-9609 for background and code allowing this to go
    // into security.json. Also see SOLR-12103.
    private static final int DEFAULT_KEYPAIR_LENGTH = 2048;

    private final int keySizeInBytes;

    /** Create an RSA key pair with newly generated keys. */
    public RSAKeyPair() {
      KeyPairGenerator keyGen;
      try {
        keyGen = KeyPairGenerator.getInstance("RSA");
      } catch (NoSuchAlgorithmException e) {
        throw new AssertionError("JVM spec is required to support RSA", e);
      }
      keyGen.initialize(DEFAULT_KEYPAIR_LENGTH);
      java.security.KeyPair keyPair = keyGen.genKeyPair();
      privateKey = keyPair.getPrivate();
      keySizeInBytes = determineKeySizeInBytes(privateKey);
      publicKey = keyPair.getPublic();
      pubKeyStr = Base64.getEncoder().encodeToString(publicKey.getEncoded());
    }

    /**
     * Initialize an RSA key pair from previously saved keys. The formats listed below have been
     * tested, other formats may also be acceptable but are not guaranteed to work.
     *
     * @param privateKeyResourceName path to private key file, encoded as a PKCS#8 in a PEM file
     * @param publicKeyResourceName path to public key file, encoded as X509 in a DER file
     * @throws IOException if an I/O error occurs reading either key file
     * @throws InvalidKeySpecException if either key file is inappropriate for an RSA key
     */
    public RSAKeyPair(URL privateKeyResourceName, URL publicKeyResourceName)
        throws IOException, InvalidKeySpecException {
      try (InputStream inPrivate = privateKeyResourceName.openStream()) {
        String privateString =
            new String(inPrivate.readAllBytes(), StandardCharsets.UTF_8)
                .replaceAll("-----(BEGIN|END) PRIVATE KEY-----", "");

        PKCS8EncodedKeySpec privateSpec =
            new PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(privateString));
        KeyFactory rsaFactory = KeyFactory.getInstance("RSA");
        privateKey = rsaFactory.generatePrivate(privateSpec);
        keySizeInBytes = determineKeySizeInBytes(privateKey);
      } catch (NoSuchAlgorithmException e) {
        throw new AssertionError("JVM spec is required to support RSA", e);
      }

      try (InputStream inPublic = publicKeyResourceName.openStream()) {
        publicKey = getX509PublicKey(inPublic.readAllBytes());
        pubKeyStr = Base64.getEncoder().encodeToString(publicKey.getEncoded());
      }
    }

    public String getPublicKeyStr() {
      return pubKeyStr;
    }

    public PublicKey getPublicKey() {
      return publicKey;
    }

    private int determineKeySizeInBytes(PrivateKey privateKey) {
      return ((RSAPrivateKey) privateKey).getModulus().bitLength() / Byte.SIZE;
    }

    // Used for testing
    public int getKeySizeInBytes() {
      return keySizeInBytes;
    }

    public byte[] encrypt(ByteBuffer buffer) {
      // This is necessary to pad the plaintext to match the exact size of the keysize in openj9.
      // OpenJDK seems to do this padding internally, but OpenJ9 does not pad the byte input to
      // the key size in bytes without padding. This only works with "RSA/ECB/nopadding".
      byte[] paddedPlaintext = new byte[getKeySizeInBytes()];
      buffer.get(paddedPlaintext, buffer.arrayOffset() + buffer.position(), buffer.limit());

      try {
        // This is better than nothing, but still not very secure
        // See:
        // https://crypto.stackexchange.com/questions/20085/which-attacks-are-possible-against-raw-textbook-rsa
        Cipher rsaCipher = Cipher.getInstance(CIPHER_ALGORITHM);
        rsaCipher.init(Cipher.ENCRYPT_MODE, privateKey);
        return rsaCipher.doFinal(paddedPlaintext);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    public byte[] signSha256(byte[] bytes) {
      Signature dsa = null;
      try {
        dsa = Signature.getInstance("SHA256withRSA");
      } catch (NoSuchAlgorithmException e) {
        throw new InternalError("SHA256withRSA is required to be supported by the JVM.", e);
      }
      try {
        dsa.initSign(privateKey);
        dsa.update(bytes, 0, bytes.length);
        return dsa.sign();
      } catch (InvalidKeyException | SignatureException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Error generating PKI Signature", e);
      }
    }
  }
}

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
package org.apache.solr.s3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

/**
 * Class representing the {@code backup} S3 config bundle specified in solr.xml. All user-provided
 * config can be overridden via environment variables SOLR_FOO_BAR or system property solr.foo.bar.
 */
public class S3BackupRepositoryConfig {

  public static final String PROFILE = "s3.profile";
  public static final String BUCKET_NAME = "s3.bucket.name";
  public static final String REGION = "s3.region";
  public static final String ENDPOINT = "s3.endpoint";
  public static final String PROXY_URL = "s3.proxy.url";
  public static final String PROXY_USE_SYSTEM_SETTINGS = "s3.proxy.useSystemSettings";
  public static final String RETRIES_DISABLE = "s3.retries.disable";
  public static final String ENCRYPTION_KMS_KEY_ID = "s3.encryption.kms.keyId";
  public static final String ENCRYPTION_KMS_REGION = "s3.encryption.kms.region";
  public static final String ENCRYPTION_KMS_ENDPOINT = "s3.encryption.kms.endpoint";
  public static final String ENCRYPTION_AES_KEY_FILE = "s3.encryption.aes.keyFile";

  static final int AES_KEY_LENGTH_BYTES = 32;

  private final String profile;
  private final String bucketName;
  private final String region;
  private final String proxyURL;
  private final boolean proxyUseSystemSettings;
  private final String endpoint;
  private final boolean disableRetries;
  private final EncryptionConfig encryption;

  public S3BackupRepositoryConfig(NamedList<?> config) {
    profile = getStringConfig(config, PROFILE);
    region = getStringConfig(config, REGION);
    bucketName = getStringConfig(config, BUCKET_NAME);
    proxyURL = getStringConfig(config, PROXY_URL);
    proxyUseSystemSettings = getBooleanConfig(config, PROXY_USE_SYSTEM_SETTINGS, true);
    endpoint = getStringConfig(config, ENDPOINT);
    disableRetries = getBooleanConfig(config, RETRIES_DISABLE, false);
    encryption = parseEncryptionConfig(config);
  }

  /** Construct a {@link S3StorageClient} from the provided config. */
  public S3StorageClient buildClient() {
    return new S3StorageClient(
        bucketName,
        profile,
        region,
        proxyURL,
        proxyUseSystemSettings,
        endpoint,
        disableRetries,
        encryption);
  }

  /** The client-side encryption settings, or {@code null} if encryption is not configured. */
  EncryptionConfig getEncryption() {
    return encryption;
  }

  /**
   * Client-side encryption settings. Exactly one of {@code aesKey} and {@code kmsKeyId} is set.
   *
   * @param kmsRegion KMS region, or {@code null} to use the S3 region
   * @param kmsEndpoint KMS endpoint override, or {@code null} for the default
   */
  record EncryptionConfig(SecretKey aesKey, String kmsKeyId, String kmsRegion, String kmsEndpoint) {
    @Override
    public String toString() {
      return aesKey != null
          ? "EncryptionConfig[AES key]"
          : "EncryptionConfig[KMS " + kmsKeyId + "]";
    }
  }

  private static EncryptionConfig parseEncryptionConfig(NamedList<?> config) {
    String kmsKeyId = getStringConfig(config, ENCRYPTION_KMS_KEY_ID);
    String kmsRegion = getStringConfig(config, ENCRYPTION_KMS_REGION);
    String kmsEndpoint = getStringConfig(config, ENCRYPTION_KMS_ENDPOINT);
    String aesKeyFile = getStringConfig(config, ENCRYPTION_AES_KEY_FILE);

    boolean useKms = StrUtils.isNotNullOrEmpty(kmsKeyId);
    boolean useAes = StrUtils.isNotNullOrEmpty(aesKeyFile);
    if (useKms && useAes) {
      throw configError(
          "Only one of "
              + ENCRYPTION_KMS_KEY_ID
              + " and "
              + ENCRYPTION_AES_KEY_FILE
              + " may be set");
    }
    if (!useKms
        && (StrUtils.isNotNullOrEmpty(kmsRegion) || StrUtils.isNotNullOrEmpty(kmsEndpoint))) {
      throw configError(
          ENCRYPTION_KMS_REGION
              + " and "
              + ENCRYPTION_KMS_ENDPOINT
              + " require "
              + ENCRYPTION_KMS_KEY_ID
              + " to be set");
    }
    if (useKms) {
      return new EncryptionConfig(
          null,
          kmsKeyId,
          StrUtils.isNullOrEmpty(kmsRegion) ? null : kmsRegion,
          StrUtils.isNullOrEmpty(kmsEndpoint) ? null : kmsEndpoint);
    }
    if (useAes) {
      return new EncryptionConfig(readAesKey(aesKeyFile), null, null, null);
    }
    return null;
  }

  /** Reads a base64-encoded 256-bit key. Error messages never include the file's content. */
  static SecretKey readAesKey(String keyFile) {
    String encoded;
    try {
      encoded = Files.readString(Path.of(keyFile), StandardCharsets.US_ASCII).strip();
    } catch (IOException e) {
      throw configError(
          "Cannot read the AES key file '" + keyFile + "' set in " + ENCRYPTION_AES_KEY_FILE, e);
    }
    byte[] key;
    try {
      key = Base64.getDecoder().decode(encoded);
    } catch (IllegalArgumentException e) {
      throw configError(
          "The AES key file '" + keyFile + "' set in " + ENCRYPTION_AES_KEY_FILE + " is not base64",
          null);
    }
    if (key.length != AES_KEY_LENGTH_BYTES) {
      throw configError(
          "The AES key in '"
              + keyFile
              + "' set in "
              + ENCRYPTION_AES_KEY_FILE
              + " must be "
              + AES_KEY_LENGTH_BYTES
              + " bytes (256 bits), base64-encoded");
    }
    return new SecretKeySpec(key, "AES");
  }

  private static SolrException configError(String message) {
    return configError(message, null);
  }

  private static SolrException configError(String message, Throwable cause) {
    return new SolrException(SolrException.ErrorCode.SERVER_ERROR, message, cause);
  }

  static String getStringConfig(NamedList<?> config, String property) {
    String envProp = EnvUtils.getProperty(property);
    if (envProp == null) {
      Object configProp = config.get(property);
      return configProp == null ? null : configProp.toString();
    } else {
      return envProp;
    }
  }

  static boolean getBooleanConfig(NamedList<?> config, String property, boolean def) {
    String envProp = EnvUtils.getProperty(property);
    if (envProp == null) {
      Boolean configProp = config.getBooleanArg(property);
      return configProp == null ? def : configProp;
    } else {
      return Boolean.parseBoolean(envProp);
    }
  }
}

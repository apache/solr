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

import static org.apache.solr.s3.S3BackupRepositoryConfig.ENCRYPTION_AES_KEY_FILE;
import static org.apache.solr.s3.S3BackupRepositoryConfig.ENCRYPTION_KMS_ENDPOINT;
import static org.apache.solr.s3.S3BackupRepositoryConfig.ENCRYPTION_KMS_KEY_ID;
import static org.apache.solr.s3.S3BackupRepositoryConfig.ENCRYPTION_KMS_REGION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.s3.S3BackupRepositoryConfig.EncryptionConfig;
import org.junit.Before;
import org.junit.Test;

/** Parsing of the client-side encryption settings. */
public class S3BackupRepositoryConfigTest extends SolrTestCase {

  private NamedList<Object> args;

  @Before
  public void setUpArgs() throws Exception {
    AbstractS3ClientTest.setS3ConfFile();
    args = new NamedList<>();
    args.add(S3BackupRepositoryConfig.BUCKET_NAME, "bucket");
    args.add(S3BackupRepositoryConfig.REGION, "us-east-1");
  }

  @Test
  public void testEncryptionOffByDefault() {
    assertNull(new S3BackupRepositoryConfig(args).getEncryption());
  }

  @Test
  public void testAesKeyFile() throws Exception {
    byte[] key = new byte[32];
    random().nextBytes(key);
    Path keyFile = writeKeyFile("  " + Base64.getEncoder().encodeToString(key) + "\n\n");
    args.add(ENCRYPTION_AES_KEY_FILE, keyFile.toString());

    EncryptionConfig encryption = new S3BackupRepositoryConfig(args).getEncryption();
    assertArrayEquals(key, encryption.aesKey().getEncoded());
    assertNull(encryption.kmsKeyId());
  }

  @Test
  public void testAesKeyFileFromSystemProperty() throws Exception {
    Path keyFile = writeKeyFile(Base64.getEncoder().encodeToString(new byte[32]));
    System.setProperty(ENCRYPTION_AES_KEY_FILE, keyFile.toString());

    assertNotNull(new S3BackupRepositoryConfig(args).getEncryption().aesKey());
  }

  @Test
  public void testAesKeyFileNotBase64() throws Exception {
    String content = "not*base64*secret";
    args.add(ENCRYPTION_AES_KEY_FILE, writeKeyFile(content).toString());

    SolrException e = expectThrows(SolrException.class, () -> new S3BackupRepositoryConfig(args));
    assertThat(e.getMessage(), containsString("is not base64"));
    assertThat(e.getMessage(), not(containsString("secret")));
  }

  @Test
  public void testAesKeyWrongLength() throws Exception {
    args.add(
        ENCRYPTION_AES_KEY_FILE,
        writeKeyFile(Base64.getEncoder().encodeToString(new byte[16])).toString());

    SolrException e = expectThrows(SolrException.class, () -> new S3BackupRepositoryConfig(args));
    assertThat(e.getMessage(), containsString("must be 32 bytes"));
  }

  @Test
  public void testAesKeyFileMissing() {
    Path missing = createTempDir().resolve("missing.key");
    args.add(ENCRYPTION_AES_KEY_FILE, missing.toString());

    SolrException e = expectThrows(SolrException.class, () -> new S3BackupRepositoryConfig(args));
    assertThat(e.getMessage(), containsString("Cannot read the AES key file"));
  }

  @Test
  public void testBothKeyOptions() throws Exception {
    args.add(ENCRYPTION_KMS_KEY_ID, "alias/backup");
    args.add(
        ENCRYPTION_AES_KEY_FILE,
        writeKeyFile(Base64.getEncoder().encodeToString(new byte[32])).toString());

    SolrException e = expectThrows(SolrException.class, () -> new S3BackupRepositoryConfig(args));
    assertThat(e.getMessage(), containsString(ENCRYPTION_KMS_KEY_ID));
    assertThat(e.getMessage(), containsString(ENCRYPTION_AES_KEY_FILE));
  }

  @Test
  public void testKmsSettingsWithoutKeyId() {
    args.add(ENCRYPTION_KMS_REGION, "eu-west-1");

    SolrException e = expectThrows(SolrException.class, () -> new S3BackupRepositoryConfig(args));
    assertThat(e.getMessage(), containsString("require " + ENCRYPTION_KMS_KEY_ID));
  }

  /** Builds and closes the clients of a KMS configuration; nothing is sent to KMS or S3. */
  @Test
  public void testKmsConfig() {
    args.add(ENCRYPTION_KMS_KEY_ID, "alias/backup");
    args.add(ENCRYPTION_KMS_REGION, "eu-west-1");
    args.add(ENCRYPTION_KMS_ENDPOINT, "http://localhost:1");

    S3BackupRepositoryConfig config = new S3BackupRepositoryConfig(args);
    EncryptionConfig encryption = config.getEncryption();
    assertNull(encryption.aesKey());
    assertEquals("alias/backup", encryption.kmsKeyId());
    assertEquals("eu-west-1", encryption.kmsRegion());
    assertEquals("http://localhost:1", encryption.kmsEndpoint());

    config.buildClient().close();
  }

  private static Path writeKeyFile(String content) throws Exception {
    Path keyFile = createTempFile("aes", ".key");
    Files.writeString(keyFile, content);
    return keyFile;
  }
}

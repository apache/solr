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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/** The incremental backup suite, with the repository configured for client-side encryption. */
@LuceneTestCase.Nightly
@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      S3MockTestcontainersThreadFilter.class,
      S3EncryptionThreadFilter.class
    })
public class S3EncryptedIncrementalBackupTest extends S3IncrementalBackupTest {

  /** Writes the key file before the cluster is configured in the superclass's setup. */
  @ClassRule
  public static final ExternalResource aesKeyFile =
      new ExternalResource() {
        @Override
        protected void before() throws Throwable {
          byte[] key = new byte[S3BackupRepositoryConfig.AES_KEY_LENGTH_BYTES];
          random().nextBytes(key);
          Path keyFile = createTempFile("aes", ".key");
          Files.writeString(keyFile, Base64.getEncoder().encodeToString(key));
          System.setProperty(AES_KEY_FILE_PROPERTY, keyFile.toString());
        }
      };

  @Test
  public void testBackupObjectsAreEncrypted() throws Exception {
    setTestSuffix("testencryptedobjects");
    final String collectionName = getCollectionName();
    final String backupName = BACKUPNAME_PREFIX + testSuffix;

    CollectionAdminRequest.createCollection(collectionName, "conf1", NUM_SHARDS, 1)
        .process(cluster.getSolrClient());
    indexDocs(collectionName, true);
    CollectionAdminRequest.backupCollection(collectionName, backupName)
        .setLocation(getBackupLocation())
        .setRepositoryName(BACKUP_REPO_NAME)
        .processAndWait(cluster.getSolrClient(), 100);

    try (S3Client s3 = s3MockContainer.createS3ClientV2()) {
      List<String> keys =
          s3
              .listObjectsV2Paginator(b -> b.bucket(BUCKET_NAME).prefix(backupName))
              .contents()
              .stream()
              .map(S3Object::key)
              .toList();
      assertTrue(
          "Backup should contain index files: " + keys,
          keys.stream().anyMatch(k -> k.contains("/index/")));
      for (String key : keys) {
        assertTrue(
            key + " should be encrypted",
            S3StorageClient.isClientSideEncrypted(
                s3.headObject(b -> b.bucket(BUCKET_NAME).key(key)).metadata()));
        try (InputStream raw = s3.getObject(b -> b.bucket(BUCKET_NAME).key(key))) {
          byte[] header = raw.readNBytes(Integer.BYTES);
          assertFalse(
              key + " should not start with a Lucene codec header",
              header.length == Integer.BYTES
                  && ByteBuffer.wrap(header).getInt() == CodecUtil.CODEC_MAGIC);
        }
      }
    }
  }
}

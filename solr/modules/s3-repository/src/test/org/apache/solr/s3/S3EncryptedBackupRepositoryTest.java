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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.After;
import org.junit.ClassRule;
import software.amazon.awssdk.regions.Region;

/** The {@link BackupRepository} contract, for a repository with client-side encryption. */
@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      S3MockTestcontainersThreadFilter.class,
      S3EncryptionThreadFilter.class
    })
public class S3EncryptedBackupRepositoryTest extends AbstractBackupRepositoryTest {

  private static final String BUCKET_NAME = S3EncryptedBackupRepositoryTest.class.getSimpleName();

  @ClassRule
  public static final S3MockContainerRule s3MockContainer = new S3MockContainerRule(BUCKET_NAME);

  private Path keyFile;

  /**
   * Repositories hold a Netty client when encrypted; the contract does not close every one it
   * creates.
   */
  private final List<S3BackupRepository> repositories = new ArrayList<>();

  @After
  public void closeRepositories() {
    IOUtils.closeWhileHandlingException(repositories);
  }

  @Override
  protected Class<? extends BackupRepository> getRepositoryClass() {
    return S3BackupRepository.class;
  }

  @Override
  protected S3BackupRepository getRepository() {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");
    S3BackupRepository repo = new S3BackupRepository();
    repositories.add(repo);
    repo.init(getBaseBackupRepositoryConfiguration());
    return repo;
  }

  @Override
  protected URI getBaseUri() throws URISyntaxException {
    return new URI("s3:/");
  }

  @Override
  protected NamedList<Object> getBaseBackupRepositoryConfiguration() {
    NamedList<Object> args = new NamedList<>();
    args.add(S3BackupRepositoryConfig.REGION, Region.US_EAST_1.id());
    args.add(S3BackupRepositoryConfig.BUCKET_NAME, BUCKET_NAME);
    args.add(S3BackupRepositoryConfig.ENDPOINT, s3MockContainer.getHttpEndpoint());
    args.add(S3BackupRepositoryConfig.ENCRYPTION_AES_KEY_FILE, keyFile().toString());
    return args;
  }

  /** One key per test, so every repository the test creates can read what the others wrote. */
  private Path keyFile() {
    if (keyFile == null) {
      byte[] key = new byte[S3BackupRepositoryConfig.AES_KEY_LENGTH_BYTES];
      random().nextBytes(key);
      try {
        keyFile = createTempFile("aes", ".key");
        Files.writeString(keyFile, Base64.getEncoder().encodeToString(key));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return keyFile;
  }
}

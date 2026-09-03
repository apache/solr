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
package org.apache.solr.azureblob;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Runs the shared {@link AbstractBackupRepositoryTest} suite against a real {@link
 * AzureBlobBackupRepository} that is created through its normal {@link
 * AzureBlobBackupRepository#init(NamedList)} code path, backed by an Azurite emulator.
 */
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      AbstractAzureBlobClientTest.OkHttpThreadLeakFilterTest.class,
    })
public class AzureBlobBackupRepositoryTest extends AbstractBackupRepositoryTest {

  private static final String CONTAINER_NAME = "test-backup-repository";

  private static AzuriteTestContainer azurite;
  private static String connectionString;

  @BeforeClass
  public static void setupClass() {
    try {
      azurite = AzuriteTestContainer.start();
    } catch (Throwable t) {
      Assume.assumeNoException("Docker/Testcontainers not available; skipping Azure tests", t);
    }
    connectionString = azurite.connectionString();
    azurite.createContainerIfMissing(CONTAINER_NAME);
  }

  @AfterClass
  public static void tearDownClass() {
    if (azurite != null) {
      azurite.stop();
      azurite = null;
    }
    connectionString = null;
  }

  @Override
  protected Class<? extends BackupRepository> getRepositoryClass() {
    return AzureBlobBackupRepository.class;
  }

  @Override
  protected BackupRepository getRepository() {
    AzureBlobBackupRepository repository = new AzureBlobBackupRepository();
    repository.init(getBaseBackupRepositoryConfiguration());
    return repository;
  }

  @Override
  protected URI getBaseUri() throws URISyntaxException {
    return new URI(AzureBlobBackupRepository.BLOB_SCHEME + ":/");
  }

  @Override
  protected NamedList<Object> getBaseBackupRepositoryConfiguration() {
    NamedList<Object> args = new NamedList<>();
    args.add(AzureBlobBackupRepositoryConfig.CONTAINER_NAME, CONTAINER_NAME);
    args.add(AzureBlobBackupRepositoryConfig.CONNECTION_STRING, connectionString);
    return args;
  }

  /**
   * Azure-specific coverage not exercised by the shared suite: an external tool (e.g. azcopy)
   * writes a child blob without this module's {@code hdi_isfolder} marker for its parent
   * "directory". {@code getPathType} reports it as a directory via prefix listing, while {@code
   * exists} resolves the exact marker-less blob and returns false. This documents the intentional
   * asymmetry; the module stays self-consistent because it always writes markers itself.
   */
  @Test
  public void testExistsVsGetPathTypeForExternalVirtualDirectory()
      throws IOException, URISyntaxException {
    BlobServiceClient serviceClient =
        new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
    BlobContainerClient containerClient = serviceClient.getBlobContainerClient(CONTAINER_NAME);
    containerClient
        .getBlobClient("external-dir/child.txt")
        .upload(BinaryData.fromString("external data"), true);

    try (BackupRepository repo = getRepository()) {
      URI dirUri = repo.resolveDirectory(getBaseUri(), "external-dir");

      assertEquals(
          "Marker-less directory should be detected as a directory",
          BackupRepository.PathType.DIRECTORY,
          repo.getPathType(dirUri));

      assertFalse(
          "exists() returns false for a marker-less external directory", repo.exists(dirUri));
    }
  }
}

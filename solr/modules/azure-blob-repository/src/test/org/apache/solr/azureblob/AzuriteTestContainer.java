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

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Lifecycle helper for a single Azurite (Azure Blob Storage emulator) Testcontainer. Used by tests
 * that cannot extend {@link AbstractAzureBlobClientTest} because they already extend a shared Solr
 * abstract test suite (e.g. the SolrCloud backup/restore and install-shard suites).
 */
final class AzuriteTestContainer {

  static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.35.0";
  static final int BLOB_SERVICE_PORT = 10000;
  static final String ACCOUNT_NAME = "devstoreaccount1";
  static final String ACCOUNT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
  private static final int HTTP_CONFLICT = 409;

  private final GenericContainer<?> container;

  private AzuriteTestContainer(GenericContainer<?> container) {
    this.container = container;
  }

  /** Starts a fresh Azurite container. Throws if Docker/Testcontainers is unavailable. */
  @SuppressWarnings("resource")
  static AzuriteTestContainer start() {
    GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse(AZURITE_IMAGE))
            .withExposedPorts(BLOB_SERVICE_PORT)
            .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck");
    container.start();
    return new AzuriteTestContainer(container);
  }

  String blobEndpoint() {
    return "http://" + container.getHost() + ":" + container.getMappedPort(BLOB_SERVICE_PORT);
  }

  String connectionString() {
    return "DefaultEndpointsProtocol=http;AccountName="
        + ACCOUNT_NAME
        + ";AccountKey="
        + ACCOUNT_KEY
        + ";BlobEndpoint="
        + blobEndpoint()
        + "/"
        + ACCOUNT_NAME
        + ";";
  }

  /** Creates the given blob container, tolerating the case where it already exists. */
  void createContainerIfMissing(String containerName) {
    BlobServiceClient serviceClient =
        new BlobServiceClientBuilder().connectionString(connectionString()).buildClient();
    try {
      serviceClient.getBlobContainerClient(containerName).create();
    } catch (BlobStorageException e) {
      if (e.getStatusCode() != HTTP_CONFLICT) {
        throw e;
      }
    }
  }

  void stop() {
    try {
      container.stop();
      container.close();
    } catch (Throwable ignored) {
      // best-effort cleanup
    }
  }
}

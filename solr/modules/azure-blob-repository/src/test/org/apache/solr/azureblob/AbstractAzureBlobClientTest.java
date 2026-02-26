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

import com.azure.core.http.HttpClient;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Abstract class for tests with Azure Blob Storage emulator. */
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      AbstractAzureBlobClientTest.OkHttpThreadLeakFilterTest.class,
    })
public class AbstractAzureBlobClientTest extends SolrTestCaseJ4 {

  private static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.33.0";
  private static final int BLOB_SERVICE_PORT = 10000;

  private static GenericContainer<?> azuriteContainer;
  private static OkHttpClient sharedOkHttpClient;
  private static String connectionString;

  protected String containerName;
  protected org.apache.solr.client.solrj.cloud.SocketProxy proxy;

  protected AzureBlobStorageClient client;

  @SuppressWarnings("resource")
  @BeforeClass
  public static void setUpClass() {
    try {
      azuriteContainer =
          new GenericContainer<>(DockerImageName.parse(AZURITE_IMAGE))
              .withExposedPorts(BLOB_SERVICE_PORT);
      azuriteContainer.start();
      sharedOkHttpClient = new OkHttpClient.Builder().build();
    } catch (Throwable t) {
      Assume.assumeNoException("Docker/Testcontainers not available; skipping Azure tests", t);
    }
  }

  @Before
  public void setUpClient() throws Exception {
    setAzureTestCredentials();

    String blobServiceUrl = getBlobServiceUrl();
    connectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint="
            + blobServiceUrl
            + "/devstoreaccount1;";

    proxy = new org.apache.solr.client.solrj.cloud.SocketProxy();
    proxy.open(new java.net.URI(blobServiceUrl));

    HttpClient httpClient = new OkHttpAsyncHttpClientBuilder(sharedOkHttpClient).build();

    String proxiedConn =
        connectionString.replace(
            ":" + azuriteContainer.getMappedPort(BLOB_SERVICE_PORT), ":" + proxy.getListenPort());

    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder()
            .connectionString(proxiedConn)
            .httpClient(httpClient)
            .buildClient();

    containerName = "test-" + java.util.UUID.randomUUID();
    client = new AzureBlobStorageClient(blobServiceClient, containerName);
  }

  public static void setAzureTestCredentials() {
    System.setProperty("AZURE_CLIENT_ID", "test-client-id");
    System.setProperty("AZURE_TENANT_ID", "test-tenant-id");
    System.setProperty("AZURE_CLIENT_SECRET", "test-client-secret");
  }

  @After
  public void tearDownClient() {
    if (client != null) {
      try {
        client.deleteContainerForTests();
      } catch (Throwable ignored) {
      }
      client.close();
    }
    if (proxy != null) {
      proxy.close();
      proxy = null;
    }
  }

  /** Simulate a connection loss on the proxy. */
  void initiateBlobConnectionLoss() {
    if (proxy != null) {
      proxy.halfClose();
    }
  }

  @AfterClass
  public static void afterAll() {
    if (azuriteContainer != null) {
      try {
        azuriteContainer.stop();
        azuriteContainer.close();
      } catch (Throwable ignored) {
      }
      azuriteContainer = null;
    }

    if (sharedOkHttpClient != null) {
      sharedOkHttpClient.dispatcher().executorService().shutdown();
      sharedOkHttpClient.dispatcher().cancelAll();
      sharedOkHttpClient.connectionPool().evictAll();
      try {
        if (sharedOkHttpClient.cache() != null) {
          sharedOkHttpClient.cache().close();
        }
      } catch (Throwable ignored) {
      }
      try {
        sharedOkHttpClient.dispatcher().executorService().awaitTermination(2, TimeUnit.SECONDS);
      } catch (Throwable ignored) {
      }
      sharedOkHttpClient = null;
    }

    try {
      reactor.core.scheduler.Schedulers.shutdownNow();
      Thread.sleep(100);
    } catch (Throwable ignored) {
    }
  }

  void pushContent(String path, String content) throws AzureBlobException {
    pushContent(path, content.getBytes(StandardCharsets.UTF_8));
  }

  void pushContent(String path, byte[] content) throws AzureBlobException {
    try (OutputStream output = client.pushStream(path)) {
      output.write(content);
    } catch (IOException e) {
      throw new AzureBlobException("Failed to write content", e);
    }
  }

  static String getConnectionString() {
    return connectionString;
  }

  String getBlobServiceUrl() {
    return "http://"
        + azuriteContainer.getHost()
        + ":"
        + azuriteContainer.getMappedPort(BLOB_SERVICE_PORT);
  }

  public static class OkHttpThreadLeakFilterTest implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
      String name = t.getName();
      if (name == null) {
        return false;
      }
      return name.contains("OkHttp") || name.contains("Okio Watchdog");
    }
  }
}

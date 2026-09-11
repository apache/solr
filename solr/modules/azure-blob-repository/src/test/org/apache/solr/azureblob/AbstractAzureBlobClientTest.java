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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import okhttp3.OkHttpClient;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.SocketProxy;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

/** Abstract class for tests with Azure Blob Storage emulator. */
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      AbstractAzureBlobClientTest.OkHttpThreadLeakFilterTest.class,
    })
public class AbstractAzureBlobClientTest extends SolrTestCase {

  private static AzuriteTestContainer azurite;
  private static OkHttpClient sharedOkHttpClient;
  private static String connectionString;

  protected String containerName;
  protected SocketProxy proxy;

  protected AzureBlobStorageClient client;

  @BeforeClass
  public static void setUpClass() {
    try {
      azurite = AzuriteTestContainer.start();
      sharedOkHttpClient = new OkHttpClient.Builder().build();
    } catch (Throwable t) {
      Assume.assumeNoException("Docker/Testcontainers not available; skipping Azure tests", t);
    }
  }

  @Before
  public void setUpClient() throws Exception {
    setAzureTestCredentials();

    URI blobServiceUri = new URI(getBlobServiceUrl());
    connectionString = azurite.connectionString();

    proxy = new SocketProxy();
    proxy.open(blobServiceUri);

    HttpClient httpClient = new OkHttpAsyncHttpClientBuilder(sharedOkHttpClient).build();

    // Route the client through the proxy so tests can simulate connection loss.
    String proxiedConn =
        connectionString.replace(":" + blobServiceUri.getPort(), ":" + proxy.getListenPort());

    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder()
            .connectionString(proxiedConn)
            .httpClient(httpClient)
            .buildClient();

    containerName = "test-" + UUID.randomUUID();
    client = new AzureBlobStorageClient(blobServiceClient, containerName);
    client.createContainerForTests();
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
    if (azurite != null) {
      azurite.stop();
      azurite = null;
    }
    sharedOkHttpClient = null;
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
    return azurite.blobEndpoint();
  }

  public static class OkHttpThreadLeakFilterTest implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
      String name = t.getName();
      if (name == null) {
        return false;
      }
      // OkHttp connection pool / dispatcher and Okio watchdog threads, plus the Reactor scheduler
      // daemon threads the Azure SDK initializes. These are process-wide and outlive individual
      // tests, so we filter them instead of force-shutting them down.
      return name.contains("OkHttp")
          || name.contains("Okio Watchdog")
          || name.startsWith("reactor-")
          || name.startsWith("parallel-")
          || name.startsWith("boundedElastic-")
          || name.startsWith("single-");
    }
  }
}

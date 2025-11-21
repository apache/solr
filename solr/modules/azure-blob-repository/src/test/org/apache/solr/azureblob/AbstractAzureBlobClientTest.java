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
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import reactor.netty.resources.ConnectionProvider;

/** Abstract class for tests with Azure Blob Storage emulator. */
public class AbstractAzureBlobClientTest extends SolrTestCaseJ4 {

  protected String containerName;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  AzureBlobStorageClient client;
  private static String connectionString;
  private EventLoopGroup eventLoopGroup;
  private ConnectionProvider connectionProvider;
  protected org.apache.solr.client.solrj.cloud.SocketProxy proxy;

  @Before
  public void setUpClient() throws Exception {
    setAzureTestCredentials();

    // Disable Netty Flight Recorder to avoid Security Manager issues
    // Keep default Netty client; OkHttp dependency not present

    // Use Azurite connection string for local testing
    connectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;";

    // Build a Netty HTTP client with isolated resources we can shut down after tests
    connectionProvider = ConnectionProvider.create("solr-azure-test");
    eventLoopGroup = new NioEventLoopGroup(1);

    // Put a proxy in front of Azurite to simulate connection loss like S3 tests
    proxy = new org.apache.solr.client.solrj.cloud.SocketProxy();
    proxy.open(new java.net.URI(getBlobServiceUrl()));

    HttpClient httpClient =
        new NettyAsyncHttpClientBuilder()
            .connectionProvider(connectionProvider)
            .eventLoopGroup(eventLoopGroup)
            .build();

    // Route Blob endpoint through the proxy by adjusting the connection string
    String proxiedConn = connectionString.replace(":10000", ":" + proxy.getListenPort());
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder()
            .connectionString(proxiedConn)
            .httpClient(httpClient)
            .buildClient();

    containerName = "test-" + java.util.UUID.randomUUID();
    client = new AzureBlobStorageClient(blobServiceClient, containerName);
  }

  /**
   * Set up Azure test credentials to avoid using real Azure credentials during testing. Similar to
   * how S3 tests use ProfileFileSystemSetting to avoid polluting the test environment.
   */
  public static void setAzureTestCredentials() {
    // Set test Azure credentials to avoid using real credentials
    System.setProperty("AZURE_CLIENT_ID", "test-client-id");
    System.setProperty("AZURE_TENANT_ID", "test-tenant-id");
    System.setProperty("AZURE_CLIENT_SECRET", "test-client-secret");

    // Set Azurite-specific environment variables
    System.setProperty(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;");
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
    try {
      reactor.core.scheduler.Schedulers.shutdownNow();
      reactor.core.scheduler.Schedulers.resetFactory();
    } catch (Throwable ignored) {
    }

    // Dispose custom Netty resources to prevent leaked threads
    try {
      if (connectionProvider != null) {
        connectionProvider.disposeLater().block();
      }
    } catch (Throwable ignored) {
    }
    try {
      if (eventLoopGroup != null) {
        eventLoopGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(3000);
      }
    } catch (Throwable ignored) {
    }
  }

  /** Simulate a connection loss on the proxy similar to S3 tests. */
  void initiateBlobConnectionLoss() throws AzureBlobException {
    if (proxy != null) {
      proxy.halfClose();
    }
  }

  @org.junit.AfterClass
  public static void afterAll() {
    try {
      reactor.core.scheduler.Schedulers.shutdownNow();
      reactor.core.scheduler.Schedulers.resetFactory();
    } catch (Throwable ignored) {
    }
  }

  /**
   * Helper method to push a string to Azure Blob Storage.
   *
   * @param path Destination path in blob storage.
   * @param content Arbitrary content for the test.
   */
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

  /** Get the connection string for tests that need direct access to the blob service. */
  static String getConnectionString() {
    return connectionString;
  }

  /** Get the blob service URL for tests that need direct access. */
  String getBlobServiceUrl() {
    return "http://localhost:10000";
  }
}

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

package org.apache.solr.client.solrj.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloudHttp2SolrClientBuilderTest extends SolrCloudTestCase {
  private static final String ANY_CHROOT = "/ANY_CHROOT";
  private static final String ANY_ZK_HOST = "ANY_ZK_HOST";
  private static final String ANY_OTHER_ZK_HOST = "ANY_OTHER_ZK_HOST";

  @BeforeClass
  public static void setupCluster() throws Exception {
    assumeWorkingMockito();
    configureCluster(1)
        .addConfig(
            "conf",
            getFile("solrj")
                .resolve("solr")
                .resolve("configsets")
                .resolve("configset-1")
                .resolve("conf"))
        .configure();
  }

  @Test
  public void testSingleZkHostSpecified() throws IOException {
    try (CloudSolrClient createdClient =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .build()) {
      try (ZkClientClusterStateProvider zkClientClusterStateProvider =
          ZkClientClusterStateProvider.from(createdClient)) {
        final String clientZkHost = zkClientClusterStateProvider.getZkHost();

        assertTrue(clientZkHost.contains(ANY_ZK_HOST));
      }
    }
  }

  @Test
  public void testSeveralZkHostsSpecifiedSingly() throws IOException {
    final List<String> zkHostList = new ArrayList<>();
    zkHostList.add(ANY_ZK_HOST);
    zkHostList.add(ANY_OTHER_ZK_HOST);
    try (CloudSolrClient createdClient =
        new CloudSolrClient.Builder(zkHostList, Optional.of(ANY_CHROOT)).build()) {
      try (ZkClientClusterStateProvider zkClientClusterStateProvider =
          ZkClientClusterStateProvider.from(createdClient)) {
        final String clientZkHost = zkClientClusterStateProvider.getZkHost();

        assertTrue(clientZkHost.contains(ANY_ZK_HOST));
        assertTrue(clientZkHost.contains(ANY_OTHER_ZK_HOST));
      }
    }
  }

  @Test
  public void testSeveralZkHostsSpecifiedTogether() throws IOException {
    final ArrayList<String> zkHosts = new ArrayList<>();
    zkHosts.add(ANY_ZK_HOST);
    zkHosts.add(ANY_OTHER_ZK_HOST);
    try (CloudSolrClient createdClient =
        new CloudSolrClient.Builder(zkHosts, Optional.of(ANY_CHROOT)).build()) {
      try (ZkClientClusterStateProvider zkClientClusterStateProvider =
          ZkClientClusterStateProvider.from(createdClient)) {
        final String clientZkHost = zkClientClusterStateProvider.getZkHost();

        assertTrue(clientZkHost.contains(ANY_ZK_HOST));
        assertTrue(clientZkHost.contains(ANY_OTHER_ZK_HOST));
      }
    }
  }

  @Test
  public void testByDefaultConfiguresClientToSendUpdatesOnlyToShardLeaders() throws IOException {
    try (CloudSolrClient createdClient =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .build()) {
      assertTrue(createdClient.isUpdatesToLeaders());
    }
  }

  @Test
  public void testIsDirectUpdatesToLeadersOnlyDefault() throws IOException {
    try (CloudSolrClient createdClient =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .build()) {
      assertFalse(createdClient.isDirectUpdatesToLeadersOnly());
    }
  }

  @Test
  public void testExternalClientAndInternalBuilderTogether() {
    expectThrows(
        IllegalStateException.class,
        () ->
            new CloudSolrClient.Builder(
                    Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
                .withHttpClient(mock(HttpJettySolrClient.class))
                .withHttpClientBuilder(mock(HttpJettySolrClient.Builder.class))
                .build());
    expectThrows(
        IllegalStateException.class,
        () ->
            new CloudSolrClient.Builder(
                    Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
                .withHttpClientBuilder(mock(HttpJettySolrClient.Builder.class))
                .withHttpClient(mock(HttpJettySolrClient.class))
                .build());
  }

  @Test
  public void testProvideInternalJettyClientBuilder() throws IOException {
    HttpJettySolrClient http2Client = mock(HttpJettySolrClient.class);
    HttpJettySolrClient.Builder http2ClientBuilder = mock(HttpJettySolrClient.Builder.class);
    when(http2ClientBuilder.build()).thenReturn(http2Client);
    CloudSolrClient.Builder clientBuilder =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withHttpClientBuilder(http2ClientBuilder);
    verify(http2ClientBuilder, never()).build();
    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(http2Client, client.getHttpClient());
      verify(http2ClientBuilder, times(1)).build();
      verify(http2Client, never()).close();
    }
    // it's internal, should be closed when closing CloudSolrClient
    verify(http2Client, times(1)).close();
  }

  @Test
  public void testProvideInternalJdkClientBuilder() throws IOException {
    var http2Client = mock(HttpJdkSolrClient.class);
    HttpJdkSolrClient.Builder http2ClientBuilder = mock(HttpJdkSolrClient.Builder.class);
    when(http2ClientBuilder.build()).thenReturn(http2Client);
    CloudSolrClient.Builder clientBuilder =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withHttpClientBuilder(http2ClientBuilder);
    verify(http2ClientBuilder, never()).build();
    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(http2Client, client.getHttpClient());
      verify(http2ClientBuilder, times(1)).build();
      verify(http2Client, never()).close();
    }
    // it's internal, should be closed when closing CloudSolrClient
    verify(http2Client, times(1)).close();
  }

  @Test
  public void testProvideExternalJettyClient() throws IOException {
    HttpJettySolrClient http2Client = mock(HttpJettySolrClient.class);
    CloudSolrClient.Builder clientBuilder =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withHttpClient(http2Client);
    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(http2Client, client.getHttpClient());
    }
    // it's external, should be NOT closed when closing CloudSolrClient
    verify(http2Client, never()).close();
  }

  @Test
  public void testProvideExternalJdkClient() throws IOException {
    HttpJdkSolrClient http2Client = mock(HttpJdkSolrClient.class);
    CloudSolrClient.Builder clientBuilder =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withHttpClient(http2Client);
    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(http2Client, client.getHttpClient());
    }
    // it's external, should be NOT closed when closing CloudSolrClient
    verify(http2Client, never()).close();
  }

  @Test
  public void testDefaultClientUsesJetty() throws IOException {
    try (CloudHttp2SolrClient createdClient =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .build()) {
      assertTrue(createdClient.getHttpClient() instanceof HttpJettySolrClient);
      assertTrue(createdClient.getLbClient().getClient(null) instanceof HttpJettySolrClient);
    }
  }

  @Test
  public void testDefaultCollectionPassedFromBuilderToClient() throws IOException {
    try (CloudHttp2SolrClient createdClient =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withDefaultCollection("aCollection")
            .build()) {
      assertEquals("aCollection", createdClient.getDefaultCollection());
    }
  }

  /**
   * Tests the consistency between the HTTP client used by {@link CloudHttp2SolrClient} and the one
   * used by its associated {@link HttpClusterStateProvider}. This method ensures that whether a
   * {@link CloudHttp2SolrClient} is created with a specific HTTP client, an internal client
   * builder, or with no specific HTTP client at all, the same HTTP client instance is used both by
   * the {@link CloudHttp2SolrClient} and by its {@link HttpClusterStateProvider}.
   */
  @Test
  public void testHttpClientPreservedInHttp2ClusterStateProvider() throws IOException {
    List<String> solrUrls = List.of(cluster.getJettySolrRunner(0).getBaseUrl().toString());

    // No httpClient - No internalClientBuilder
    testHttpClientConsistency(solrUrls, null, null);

    // httpClient - No internalClientBuilder
    try (var httpClient = new HttpJettySolrClient.Builder().build()) {
      testHttpClientConsistency(solrUrls, httpClient, null);
    }

    // No httpClient - internalClientBuilder
    var internalClientBuilder = new HttpJettySolrClient.Builder();
    testHttpClientConsistency(solrUrls, null, internalClientBuilder);
  }

  private void testHttpClientConsistency(
      List<String> solrUrls,
      HttpJettySolrClient httpClient,
      HttpJettySolrClient.Builder internalClientBuilder)
      throws IOException {
    CloudSolrClient.Builder clientBuilder = new CloudSolrClient.Builder(solrUrls);

    if (httpClient != null) {
      clientBuilder.withHttpClient(httpClient);
    } else if (internalClientBuilder != null) {
      clientBuilder.withHttpClientBuilder(internalClientBuilder);
    }

    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(
          client.getHttpClient(),
          ((HttpClusterStateProvider) client.getClusterStateProvider()).getHttpClient());
    }
  }

  public void testCustomStateProvider() throws IOException {
    ZkClientClusterStateProvider stateProvider = mock(ZkClientClusterStateProvider.class);
    try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(stateProvider).build()) {
      assertEquals(stateProvider, cloudSolrClient.getClusterStateProvider());
    }
    verify(stateProvider, times(1)).close();
  }

  @Test
  @SuppressWarnings({"try"})
  public void test0Timeouts() throws IOException {
    try (CloudSolrClient createdClient =
        new CloudSolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.empty())
            .withZkConnectTimeout(0, TimeUnit.MILLISECONDS)
            .withZkClientTimeout(0, TimeUnit.MILLISECONDS)
            .build()) {
      assertNotNull(createdClient);
    }
  }
}

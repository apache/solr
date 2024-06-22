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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

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
                .toPath()
                .resolve("solr")
                .resolve("configsets")
                .resolve("configset-1")
                .resolve("conf"))
        .configure();
  }

  @Test
  public void testSingleZkHostSpecified() throws IOException {
    try (CloudSolrClient createdClient =
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
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
        new CloudHttp2SolrClient.Builder(zkHostList, Optional.of(ANY_CHROOT)).build()) {
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
        new CloudHttp2SolrClient.Builder(zkHosts, Optional.of(ANY_CHROOT)).build()) {
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
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .build()) {
      assertTrue(createdClient.isUpdatesToLeaders());
    }
  }

  @Test
  public void testIsDirectUpdatesToLeadersOnlyDefault() throws IOException {
    try (CloudSolrClient createdClient =
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .build()) {
      assertFalse(createdClient.isDirectUpdatesToLeadersOnly());
    }
  }

  @Test
  public void testExternalClientAndInternalBuilderTogether() {
    expectThrows(
        IllegalStateException.class,
        () ->
            new CloudHttp2SolrClient.Builder(
                    Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
                .withHttpClient(Mockito.mock(Http2SolrClient.class))
                .withInternalClientBuilder(Mockito.mock(Http2SolrClient.Builder.class))
                .build());
    expectThrows(
        IllegalStateException.class,
        () ->
            new CloudHttp2SolrClient.Builder(
                    Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
                .withInternalClientBuilder(Mockito.mock(Http2SolrClient.Builder.class))
                .withHttpClient(Mockito.mock(Http2SolrClient.class))
                .build());
  }

  @Test
  public void testProvideInternalBuilder() throws IOException {
    Http2SolrClient http2Client = Mockito.mock(Http2SolrClient.class);
    Http2SolrClient.Builder http2ClientBuilder = Mockito.mock(Http2SolrClient.Builder.class);
    when(http2ClientBuilder.build()).thenReturn(http2Client);
    CloudHttp2SolrClient.Builder clientBuilder =
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withInternalClientBuilder(http2ClientBuilder);
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
  public void testProvideExternalClient() throws IOException {
    Http2SolrClient http2Client = Mockito.mock(Http2SolrClient.class);
    CloudHttp2SolrClient.Builder clientBuilder =
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withHttpClient(http2Client);
    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(http2Client, client.getHttpClient());
    }
    // it's external, should be NOT closed when closing CloudSolrClient
    verify(http2Client, never()).close();
  }

  @Test
  public void testDefaultCollectionPassedFromBuilderToClient() throws IOException {
    try (CloudHttp2SolrClient createdClient =
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withDefaultCollection("aCollection")
            .build()) {
      assertEquals("aCollection", createdClient.getDefaultCollection());
    }
  }

  /**
   * Tests the consistency between the HTTP client used by {@link CloudHttp2SolrClient} and the one
   * used by its associated {@link Http2ClusterStateProvider}. This method ensures that whether a
   * {@link CloudHttp2SolrClient} is created with a specific HTTP client, an internal client
   * builder, or with no specific HTTP client at all, the same HTTP client instance is used both by
   * the {@link CloudHttp2SolrClient} and by its {@link Http2ClusterStateProvider}.
   */
  @Test
  public void testHttpClientPreservedInHttp2ClusterStateProvider() throws IOException {
    List<String> solrUrls = List.of(cluster.getJettySolrRunner(0).getBaseUrl().toString());

    // No httpClient - No internalClientBuilder
    testHttpClientConsistency(solrUrls, null, null);

    // httpClient - No internalClientBuilder
    try (Http2SolrClient httpClient = new Http2SolrClient.Builder().build()) {
      testHttpClientConsistency(solrUrls, httpClient, null);
    }

    // No httpClient - internalClientBuilder
    Http2SolrClient.Builder internalClientBuilder = new Http2SolrClient.Builder();
    testHttpClientConsistency(solrUrls, null, internalClientBuilder);
  }

  private void testHttpClientConsistency(
      List<String> solrUrls,
      Http2SolrClient httpClient,
      Http2SolrClient.Builder internalClientBuilder)
      throws IOException {
    CloudHttp2SolrClient.Builder clientBuilder = new CloudHttp2SolrClient.Builder(solrUrls);

    if (httpClient != null) {
      clientBuilder.withHttpClient(httpClient);
    } else if (internalClientBuilder != null) {
      clientBuilder.withInternalClientBuilder(internalClientBuilder);
    }

    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(
          client.getHttpClient(),
          ((Http2ClusterStateProvider) client.getClusterStateProvider()).getHttpClient());
    }
  }
}

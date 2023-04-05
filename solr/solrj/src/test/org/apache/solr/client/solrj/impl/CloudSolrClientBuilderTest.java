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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.junit.Test;

public class CloudSolrClientBuilderTest extends SolrTestCase {
  private static final String ANY_CHROOT = "/ANY_CHROOT";
  private static final String ANY_ZK_HOST = "ANY_ZK_HOST";
  private static final String ANY_OTHER_ZK_HOST = "ANY_OTHER_ZK_HOST";

  @Test
  public void testSingleZkHostSpecified() throws IOException {
    try (CloudSolrClient createdClient =
        new Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT)).build(); ) {
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
    try (CloudSolrClient createdClient = new Builder(zkHostList, Optional.of(ANY_CHROOT)).build()) {
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
    try (CloudSolrClient createdClient = new Builder(zkHosts, Optional.of(ANY_CHROOT)).build()) {
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
        new Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT)).build()) {
      assertTrue(createdClient.isUpdatesToLeaders());
    }
  }

  @Test
  public void testIsDirectUpdatesToLeadersOnlyDefault() throws IOException {
    try (CloudSolrClient createdClient =
        new Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT)).build()) {
      assertFalse(createdClient.isDirectUpdatesToLeadersOnly());
    }
  }

  @Test
  @SuppressWarnings({"try"})
  public void test0Timeouts() throws IOException {
    try (CloudSolrClient createdClient =
        new CloudLegacySolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.empty())
            .withSocketTimeout(0, TimeUnit.MILLISECONDS)
            .withConnectionTimeout(0, TimeUnit.MILLISECONDS)
            .build()) {
      assertNotNull(createdClient);
    }
  }
}

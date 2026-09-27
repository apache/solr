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

package org.apache.solr.crossdc.manager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;

/** Shared Mockito helper for tests that mock a {@link CloudSolrClient}. */
public final class CrossDcMockUtils {

  private CrossDcMockUtils() {}

  /**
   * Mocks a client with a mocked cluster-state provider. {@code SolrMessageProcessor} reads the
   * live-node list from this provider before processing a request. Tests can retrieve the provider
   * with {@link CloudSolrClient#getClusterStateProvider()} to stub or verify it.
   */
  public static CloudSolrClient mockCloudSolrClientWithClusterStateProvider() {
    CloudSolrClient client = mock(CloudSolrClient.class);
    when(client.getClusterStateProvider()).thenReturn(mock(ClusterStateProvider.class));
    return client;
  }
}

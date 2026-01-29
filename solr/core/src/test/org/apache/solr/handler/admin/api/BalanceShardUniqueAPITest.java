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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.ONLY_ACTIVE_NODES;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SHARD_UNIQUE;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.mockito.Mockito.when;

import org.apache.solr.client.api.model.BalanceShardUniqueRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link BalanceShardUnique} */
public class BalanceShardUniqueAPITest extends MockV2APITest {

  private BalanceShardUnique api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new BalanceShardUnique(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.balanceShardUnique("someCollectionName", null));

    assertEquals(400, thrown.code());
    assertEquals("Missing required request body", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    final var requestBody = new BalanceShardUniqueRequestBody();
    requestBody.property = "preferredLeader";
    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.balanceShardUnique(null, requestBody));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: collection", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfPropertyToBalanceIsMissing() {
    // Note, 'property' param on reqBody not set
    final var requestBody = new BalanceShardUniqueRequestBody();
    final SolrException thrown =
        expectThrows(
            SolrException.class, () -> api.balanceShardUnique("someCollName", requestBody));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: property", thrown.getMessage());
  }

  @Test
  public void testCreateRemoteMessageAllProperties() throws Exception {
    final var requestBody = new BalanceShardUniqueRequestBody();
    requestBody.property = "someProperty";
    requestBody.shardUnique = Boolean.TRUE;
    requestBody.onlyActiveNodes = Boolean.TRUE;
    requestBody.async = "someAsyncId";

    api.balanceShardUnique("someCollName", requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.BALANCESHARDUNIQUE,
        requestBody.async,
        message -> {
          assertEquals(4, message.size());
          assertEquals("someCollName", message.get(COLLECTION));
          assertEquals("someProperty", message.get(PROPERTY_PROP));
          assertEquals(Boolean.TRUE, message.get(SHARD_UNIQUE));
          assertEquals(Boolean.TRUE, message.get(ONLY_ACTIVE_NODES));
        });
  }
}

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

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.ONLY_ACTIVE_NODES;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SHARD_UNIQUE;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.Test;

/** Unit tests for {@link BalanceShardUniqueAPI} */
public class BalanceShardUniqueAPITest extends SolrTestCaseJ4 {

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new BalanceShardUniqueAPI(null, null, null);
              api.balanceShardUnique("someCollectionName", null);
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required request body", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    final var requestBody = new BalanceShardUniqueAPI.BalanceShardUniqueRequestBody();
    requestBody.property = "preferredLeader";
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new BalanceShardUniqueAPI(null, null, null);
              api.balanceShardUnique(null, requestBody);
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: collection", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfPropertyToBalanceIsMissing() {
    // Note, 'property' param on reqBody not set
    final var requestBody = new BalanceShardUniqueAPI.BalanceShardUniqueRequestBody();
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new BalanceShardUniqueAPI(null, null, null);
              api.balanceShardUnique("someCollName", requestBody);
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: property", thrown.getMessage());
  }

  @Test
  public void testCreateRemoteMessageAllProperties() {
    final var requestBody = new BalanceShardUniqueAPI.BalanceShardUniqueRequestBody();
    requestBody.property = "someProperty";
    requestBody.shardUnique = Boolean.TRUE;
    requestBody.onlyActiveNodes = Boolean.TRUE;
    requestBody.asyncId = "someAsyncId";
    final var remoteMessage =
        BalanceShardUniqueAPI.createRemoteMessage("someCollName", requestBody).getProperties();

    assertEquals(6, remoteMessage.size());
    assertEquals("balanceshardunique", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals("someProperty", remoteMessage.get(PROPERTY_PROP));
    assertEquals(Boolean.TRUE, remoteMessage.get(SHARD_UNIQUE));
    assertEquals(Boolean.TRUE, remoteMessage.get(ONLY_ACTIVE_NODES));
    assertEquals("someAsyncId", remoteMessage.get(ASYNC));
  }
}

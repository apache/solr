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

import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INDEX;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INSTANCE_DIR;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link DeleteShard} */
public class DeleteShardAPITest extends MockV2APITest {

  private DeleteShard api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new DeleteShard(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> api.deleteShard(null, "someShard", null, null, null, null, null));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: collection", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfShardNameMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> api.deleteShard("someCollection", null, null, null, null, null, null));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: shard", thrown.getMessage());
  }

  @Test
  public void testCreateRemoteMessageAllProperties() throws Exception {
    when(mockClusterState.hasCollection(eq("someCollectionName"))).thenReturn(true);
    api.deleteShard("someCollName", "someShardName", true, false, true, false, "someAsyncId");

    validateRunCommand(
        CollectionParams.CollectionAction.DELETESHARD,
        "someAsyncId",
        message -> {
          assertEquals(6, message.size());
          assertEquals("someCollName", message.get(COLLECTION));
          assertEquals("someShardName", message.get(SHARD_ID_PROP));
          assertEquals(Boolean.TRUE, message.get(DELETE_INSTANCE_DIR));
          assertEquals(Boolean.FALSE, message.get(DELETE_DATA_DIR));
          assertEquals(Boolean.TRUE, message.get(DELETE_INDEX));
          assertEquals(Boolean.FALSE, message.get(FOLLOW_ALIASES));
        });
  }

  @Test
  public void testMissingValuesExcludedFromRemoteMessage() throws Exception {
    when(mockClusterState.hasCollection(eq("someCollectionName"))).thenReturn(true);
    api.deleteShard("someCollName", "someShardName", null, null, null, null, null);

    validateRunCommand(
        CollectionParams.CollectionAction.DELETESHARD,
        message -> {
          assertEquals(2, message.size());
          assertEquals("someCollName", message.get(COLLECTION));
          assertEquals("someShardName", message.get(SHARD_ID_PROP));
        });
  }
}

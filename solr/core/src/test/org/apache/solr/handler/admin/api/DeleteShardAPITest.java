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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
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
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();

    assertEquals(6, remoteMessage.size());
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));
    assertEquals(Boolean.TRUE, remoteMessage.get(DELETE_INSTANCE_DIR));
    assertEquals(Boolean.FALSE, remoteMessage.get(DELETE_DATA_DIR));
    assertEquals(Boolean.TRUE, remoteMessage.get(DELETE_INDEX));
    assertEquals(Boolean.FALSE, remoteMessage.get(FOLLOW_ALIASES));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETESHARD, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testMissingValuesExcludedFromRemoteMessage() throws Exception {
    when(mockClusterState.hasCollection(eq("someCollectionName"))).thenReturn(true);
    api.deleteShard("someCollName", "someShardName", null, null, null, null, null);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();

    assertEquals(2, remoteMessage.size());
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETESHARD, context.getAction());
    assertNull("There should be no asyncId", context.getAsyncId());
  }
}

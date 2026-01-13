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

import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.ONLY_IF_DOWN;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.COUNT_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.REPLICA;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INDEX;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INSTANCE_DIR;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.client.api.model.ScaleCollectionRequestBody;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link DeleteReplica} */
public class DeleteReplicaAPITest extends MockAPITest {

  private DeleteReplica api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new DeleteReplica(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () ->
                api.deleteReplicaByName(
                    null, "someShard", "someReplica", null, null, null, null, null, null));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: collection", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfShardNameMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () ->
                api.deleteReplicaByName(
                    "someCollection", null, "someReplica", null, null, null, null, null, null));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: shard", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfReplicaNameMissingWhenDeletingByName() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () ->
                api.deleteReplicaByName(
                    "someCollection", "someShard", null, null, null, null, null, null, null));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: replica", thrown.getMessage());
  }

  @Test
  public void testCreateRemoteMessageByName() throws Exception {
    api.deleteReplicaByName(
        "someCollName",
        "someShardName",
        "someReplicaName",
        true,
        false,
        true,
        false,
        true,
        "someAsyncId");
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps message = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = message.getProperties();
    assertEquals(8, remoteMessage.size());
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));
    assertEquals("someReplicaName", remoteMessage.get(REPLICA));
    assertEquals(Boolean.TRUE, remoteMessage.get(FOLLOW_ALIASES));
    assertEquals(Boolean.FALSE, remoteMessage.get(DELETE_INSTANCE_DIR));
    assertEquals(Boolean.TRUE, remoteMessage.get(DELETE_DATA_DIR));
    assertEquals(Boolean.FALSE, remoteMessage.get(DELETE_INDEX));
    assertEquals(Boolean.TRUE, remoteMessage.get(ONLY_IF_DOWN));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETEREPLICA, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testCreateRemoteMessageByCount() throws Exception {
    api.deleteReplicasByCount(
        "someCollName", "someShardName", 123, true, false, true, false, true, "someAsyncId");
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps message = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = message.getProperties();
    assertEquals(8, remoteMessage.size());
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));
    assertEquals(123, remoteMessage.get(COUNT_PROP));
    assertEquals(Boolean.TRUE, remoteMessage.get(FOLLOW_ALIASES));
    assertEquals(Boolean.FALSE, remoteMessage.get(DELETE_INSTANCE_DIR));
    assertEquals(Boolean.TRUE, remoteMessage.get(DELETE_DATA_DIR));
    assertEquals(Boolean.FALSE, remoteMessage.get(DELETE_INDEX));
    assertEquals(Boolean.TRUE, remoteMessage.get(ONLY_IF_DOWN));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETEREPLICA, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testCreateRemoteMessageByCountAllShards() throws Exception {
    ScaleCollectionRequestBody body = new ScaleCollectionRequestBody();
    body.numToDelete = 123;
    body.followAliases = true;
    body.deleteIndex = true;
    body.onlyIfDown = false;
    body.async = "someAsyncId";
    api.deleteReplicasByCountAllShards("someCollName", body);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps message = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = message.getProperties();
    assertEquals(5, remoteMessage.size());
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals(123, remoteMessage.get(COUNT_PROP));
    assertEquals(Boolean.TRUE, remoteMessage.get(FOLLOW_ALIASES));
    assertEquals(Boolean.TRUE, remoteMessage.get(DELETE_INDEX));
    assertEquals(Boolean.FALSE, remoteMessage.get(ONLY_IF_DOWN));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETEREPLICA, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testMissingValuesExcludedFromRemoteMessage() throws Exception {
    api.deleteReplicaByName(
        "someCollName", "someShardName", "someReplicaName", null, null, null, null, null, null);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps message = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = message.getProperties();
    assertEquals(3, remoteMessage.size());
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));
    assertEquals("someReplicaName", remoteMessage.get(REPLICA));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETEREPLICA, context.getAction());
    assertNull("AsyncId should be null", context.getAsyncId());
  }
}

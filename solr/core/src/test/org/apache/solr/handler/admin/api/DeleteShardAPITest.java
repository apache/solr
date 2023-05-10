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
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INDEX;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INSTANCE_DIR;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.Test;

/** Unit tests for {@link DeleteShardAPI} */
public class DeleteShardAPITest extends SolrTestCaseJ4 {
  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new DeleteShardAPI(null, null, null);
              api.deleteShard(null, "someShard", null, null, null, null, null);
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: collection", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfShardNameMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new DeleteShardAPI(null, null, null);
              api.deleteShard("someCollection", null, null, null, null, null, null);
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: shard", thrown.getMessage());
  }

  @Test
  public void testCreateRemoteMessageAllProperties() {
    final var remoteMessage =
        DeleteShardAPI.createRemoteMessage(
                "someCollName", "someShardName", true, false, true, false, "someAsyncId")
            .getProperties();

    assertEquals(8, remoteMessage.size());
    assertEquals("deleteshard", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));
    assertEquals(Boolean.TRUE, remoteMessage.get(DELETE_INSTANCE_DIR));
    assertEquals(Boolean.FALSE, remoteMessage.get(DELETE_DATA_DIR));
    assertEquals(Boolean.TRUE, remoteMessage.get(DELETE_INDEX));
    assertEquals(Boolean.FALSE, remoteMessage.get(FOLLOW_ALIASES));
    assertEquals("someAsyncId", remoteMessage.get(ASYNC));
  }

  @Test
  public void testMissingValuesExcludedFromRemoteMessage() {
    final var remoteMessage =
        DeleteShardAPI.createRemoteMessage(
                "someCollName", "someShardName", null, null, null, null, null)
            .getProperties();

    assertEquals(3, remoteMessage.size());
    assertEquals("deleteshard", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someCollName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));
  }
}

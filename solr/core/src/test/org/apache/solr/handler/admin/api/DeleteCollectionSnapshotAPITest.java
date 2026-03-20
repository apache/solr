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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DeleteCollectionSnapshotAPITest extends MockV2APITest {

  private DeleteCollectionSnapshot api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new DeleteCollectionSnapshot(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testConstructsValidOverseerMessage() throws Exception {
    when(mockClusterState.hasCollection("myCollName")).thenReturn(true);
    when(mockSolrZkClient.exists(anyString())).thenReturn(false);

    api.deleteCollectionSnapshot("myCollName", "mySnapshotName", false, null);

    validateRunCommand(
        CollectionParams.CollectionAction.DELETESNAPSHOT,
        message -> {
          assertEquals(3, message.size());
          assertThat(
              message.keySet(),
              containsInAnyOrder(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME, FOLLOW_ALIASES));
          assertEquals("myCollName", message.get(COLLECTION_PROP));
          assertEquals("mySnapshotName", message.get(CoreAdminParams.COMMIT_NAME));
          assertEquals(false, message.get(FOLLOW_ALIASES));
        });

    Mockito.clearInvocations(mockCommandRunner);
    api.deleteCollectionSnapshot("myCollName", "mySnapshotName", true, "testId");

    validateRunCommand(
        CollectionParams.CollectionAction.DELETESNAPSHOT,
        "testId",
        message -> {
          assertEquals(3, message.size());
          assertThat(
              message.keySet(),
              containsInAnyOrder(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME, FOLLOW_ALIASES));
          assertEquals("myCollName", message.get(COLLECTION_PROP));
          assertEquals("mySnapshotName", message.get(CoreAdminParams.COMMIT_NAME));
          assertEquals(true, message.get(FOLLOW_ALIASES));
        });
  }
}

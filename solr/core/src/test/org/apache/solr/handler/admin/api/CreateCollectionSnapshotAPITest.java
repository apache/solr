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

import org.apache.solr.client.api.model.CreateCollectionSnapshotRequestBody;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CreateCollectionSnapshotAPITest extends MockV2APITest {

  private CreateCollectionSnapshot api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new CreateCollectionSnapshot(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testConstructsValidOverseerMessage() throws Exception {
    when(mockClusterState.hasCollection("myCollName")).thenReturn(true);
    when(mockSolrZkClient.exists(anyString())).thenReturn(false);

    CreateCollectionSnapshotRequestBody body = new CreateCollectionSnapshotRequestBody();
    body.followAliases = false;

    api.createCollectionSnapshot("myCollName", "mySnapshotName", body);

    validateRunCommand(
        CollectionParams.CollectionAction.CREATESNAPSHOT,
        message -> {
          assertEquals(3, message.size());
          assertThat(
              message.keySet(),
              containsInAnyOrder(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME, FOLLOW_ALIASES));
          assertEquals("myCollName", message.get(COLLECTION_PROP));
          assertEquals("mySnapshotName", message.get(CoreAdminParams.COMMIT_NAME));
          assertEquals(false, message.get(FOLLOW_ALIASES));
        });

    body.followAliases = true;
    body.async = "testId";
    Mockito.clearInvocations(mockCommandRunner);
    api.createCollectionSnapshot("myCollName", "mySnapshotName", body);

    validateRunCommand(
        CollectionParams.CollectionAction.CREATESNAPSHOT,
        body.async,
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

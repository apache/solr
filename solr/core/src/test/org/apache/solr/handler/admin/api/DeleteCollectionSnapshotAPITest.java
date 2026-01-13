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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DeleteCollectionSnapshotAPITest extends MockAPITest {

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
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());
    final ZkNodeProps messageOne = messageCapturer.getValue();
    final Map<String, Object> rawMessageOne = messageOne.getProperties();
    assertEquals(3, rawMessageOne.size());
    assertThat(
        rawMessageOne.keySet(),
        containsInAnyOrder(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME, FOLLOW_ALIASES));
    assertEquals("myCollName", rawMessageOne.get(COLLECTION_PROP));
    assertEquals("mySnapshotName", rawMessageOne.get(CoreAdminParams.COMMIT_NAME));
    assertEquals(false, rawMessageOne.get(FOLLOW_ALIASES));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETESNAPSHOT, context.getAction());
    assertNull(context.getAsyncId());

    Mockito.clearInvocations(mockCommandRunner);
    api.deleteCollectionSnapshot("myCollName", "mySnapshotName", true, "testId");
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());
    final ZkNodeProps messageTwo = messageCapturer.getValue();
    final Map<String, Object> rawMessageTwo = messageTwo.getProperties();
    assertEquals(3, rawMessageTwo.size());
    assertThat(
        rawMessageTwo.keySet(),
        containsInAnyOrder(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME, FOLLOW_ALIASES));
    assertEquals("myCollName", rawMessageTwo.get(COLLECTION_PROP));
    assertEquals("mySnapshotName", rawMessageTwo.get(CoreAdminParams.COMMIT_NAME));
    assertEquals(true, rawMessageTwo.get(FOLLOW_ALIASES));

    context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETESNAPSHOT, context.getAction());
    assertEquals("testId", context.getAsyncId());
  }
}

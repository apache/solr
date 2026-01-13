/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.admin.api;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.api.model.MigrateReplicasRequestBody;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link ReplaceNode} */
public class MigrateReplicasAPITest extends MockAPITest {

  private MigrateReplicas migrateReplicasAPI;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    migrateReplicasAPI = new MigrateReplicas(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testCreatesValidOverseerMessage() throws Exception {
    MigrateReplicasRequestBody requestBody =
        new MigrateReplicasRequestBody(
            Set.of("demoSourceNode"), Set.of("demoTargetNode"), false, "async");
    migrateReplicasAPI.migrateReplicas(requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(3, createdMessageProps.size());
    assertEquals(Set.of("demoSourceNode"), createdMessageProps.get("sourceNodes"));
    assertEquals(Set.of("demoTargetNode"), createdMessageProps.get("targetNodes"));
    assertEquals(false, createdMessageProps.get("waitForFinalState"));
    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.MIGRATE_REPLICAS, context.getAction());
    assertEquals("async", context.getAsyncId());
  }

  @Test
  public void testNoTargetNodes() throws Exception {
    MigrateReplicasRequestBody requestBody =
        new MigrateReplicasRequestBody(Set.of("demoSourceNode"), null, null, null);
    migrateReplicasAPI.migrateReplicas(requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(1, createdMessageProps.size());
    assertEquals(Set.of("demoSourceNode"), createdMessageProps.get("sourceNodes"));
    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.MIGRATE_REPLICAS, context.getAction());
    assertNull("There should be no asyncId", context.getAsyncId());
  }

  @Test
  public void testNoSourceNodesThrowsError() throws Exception {
    MigrateReplicasRequestBody requestBody1 =
        new MigrateReplicasRequestBody(
            Collections.emptySet(), Set.of("demoTargetNode"), null, null);
    assertThrows(SolrException.class, () -> migrateReplicasAPI.migrateReplicas(requestBody1));
    MigrateReplicasRequestBody requestBody2 =
        new MigrateReplicasRequestBody(null, Set.of("demoTargetNode"), null, null);
    assertThrows(SolrException.class, () -> migrateReplicasAPI.migrateReplicas(requestBody2));
  }
}

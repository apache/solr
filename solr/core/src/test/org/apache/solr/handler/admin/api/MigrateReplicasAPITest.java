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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.MigrateReplicasRequestBody;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link ReplaceNode} */
public class MigrateReplicasAPITest extends SolrTestCaseJ4 {

  private CoreContainer mockCoreContainer;
  private SolrQueryRequest mockQueryRequest;
  private SolrQueryResponse queryResponse;
  private MigrateReplicas migrateReplicasAPI;
  private DistributedCollectionConfigSetCommandRunner mockCommandRunner;
  private ArgumentCaptor<ZkNodeProps> messageCapturer;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    mockCoreContainer = mock(CoreContainer.class);
    mockCommandRunner = mock(DistributedCollectionConfigSetCommandRunner.class);
    when(mockCoreContainer.getDistributedCollectionCommandRunner())
        .thenReturn(Optional.of(mockCommandRunner));
    when(mockCommandRunner.runCollectionCommand(any(), any(), anyLong()))
        .thenReturn(new OverseerSolrResponse(new NamedList<>()));
    mockQueryRequest = mock(SolrQueryRequest.class);
    queryResponse = new SolrQueryResponse();
    migrateReplicasAPI = new MigrateReplicas(mockCoreContainer, mockQueryRequest, queryResponse);
    messageCapturer = ArgumentCaptor.forClass(ZkNodeProps.class);

    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
  }

  @Test
  public void testCreatesValidOverseerMessage() throws Exception {
    MigrateReplicasRequestBody requestBody =
        new MigrateReplicasRequestBody(
            Set.of("demoSourceNode"), Set.of("demoTargetNode"), false, "async");
    migrateReplicasAPI.migrateReplicas(requestBody);
    verify(mockCommandRunner).runCollectionCommand(messageCapturer.capture(), any(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(5, createdMessageProps.size());
    assertEquals(Set.of("demoSourceNode"), createdMessageProps.get("sourceNodes"));
    assertEquals(Set.of("demoTargetNode"), createdMessageProps.get("targetNodes"));
    assertEquals(false, createdMessageProps.get("waitForFinalState"));
    assertEquals("async", createdMessageProps.get("async"));
    assertEquals("migrate_replicas", createdMessageProps.get("operation"));
  }

  @Test
  public void testNoTargetNodes() throws Exception {
    MigrateReplicasRequestBody requestBody =
        new MigrateReplicasRequestBody(Set.of("demoSourceNode"), null, null, null);
    migrateReplicasAPI.migrateReplicas(requestBody);
    verify(mockCommandRunner).runCollectionCommand(messageCapturer.capture(), any(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(2, createdMessageProps.size());
    assertEquals(Set.of("demoSourceNode"), createdMessageProps.get("sourceNodes"));
    assertEquals("migrate_replicas", createdMessageProps.get("operation"));
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

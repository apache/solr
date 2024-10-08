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

import java.util.Map;
import java.util.Optional;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.ReplaceNodeRequestBody;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
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
public class ReplaceNodeAPITest extends SolrTestCaseJ4 {

  private CoreContainer mockCoreContainer;
  private SolrQueryRequest mockQueryRequest;
  private SolrQueryResponse queryResponse;
  private ReplaceNode replaceNodeApi;
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
    replaceNodeApi = new ReplaceNode(mockCoreContainer, mockQueryRequest, queryResponse);
    messageCapturer = ArgumentCaptor.forClass(ZkNodeProps.class);

    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
  }

  @Test
  public void testCreatesValidOverseerMessage() throws Exception {
    final var requestBody = new ReplaceNodeRequestBody("demoTargetNode", false, "async");
    replaceNodeApi.replaceNode("demoSourceNode", requestBody);
    verify(mockCommandRunner).runCollectionCommand(messageCapturer.capture(), any(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(5, createdMessageProps.size());
    assertEquals("demoSourceNode", createdMessageProps.get("sourceNode"));
    assertEquals("demoTargetNode", createdMessageProps.get("targetNode"));
    assertEquals(false, createdMessageProps.get("waitForFinalState"));
    assertEquals("async", createdMessageProps.get("async"));
    assertEquals("replacenode", createdMessageProps.get("operation"));
  }

  @Test
  public void testRequestBodyCanBeOmittedAltogether() throws Exception {
    replaceNodeApi.replaceNode("demoSourceNode", null);
    verify(mockCommandRunner).runCollectionCommand(messageCapturer.capture(), any(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(2, createdMessageProps.size());
    assertEquals("demoSourceNode", createdMessageProps.get("sourceNode"));
    assertEquals("replacenode", createdMessageProps.get("operation"));
  }

  @Test
  public void testOptionalValuesNotAddedToRemoteMessageIfNotProvided() throws Exception {
    final var requestBody = new ReplaceNodeRequestBody("demoTargetNode", null, null);
    replaceNodeApi.replaceNode("demoSourceNode", requestBody);
    verify(mockCommandRunner).runCollectionCommand(messageCapturer.capture(), any(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();

    assertEquals(3, createdMessageProps.size());
    assertEquals("demoSourceNode", createdMessageProps.get("sourceNode"));
    assertEquals("demoTargetNode", createdMessageProps.get("targetNode"));
    assertEquals("replacenode", createdMessageProps.get("operation"));
    assertFalse(
        "Expected message to not contain value for waitForFinalState: "
            + createdMessageProps.get("waitForFinalState"),
        createdMessageProps.containsKey("waitForFinalState"));
    assertFalse(
        "Expected message to not contain value for async: " + createdMessageProps.get("async"),
        createdMessageProps.containsKey("async"));
  }
}

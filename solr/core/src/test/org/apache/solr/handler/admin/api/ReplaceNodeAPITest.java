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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.client.api.model.ReplaceNodeRequestBody;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link ReplaceNode} */
public class ReplaceNodeAPITest extends MockAPITest {

  private ReplaceNode api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new ReplaceNode(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testCreatesValidOverseerMessage() throws Exception {
    final var requestBody = new ReplaceNodeRequestBody("demoTargetNode", false, "async");
    api.replaceNode("demoSourceNode", requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(3, createdMessageProps.size());
    assertEquals("demoSourceNode", createdMessageProps.get("sourceNode"));
    assertEquals("demoTargetNode", createdMessageProps.get("targetNode"));
    assertEquals(false, createdMessageProps.get("waitForFinalState"));
    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.REPLACENODE, context.getAction());
    assertEquals("async", context.getAsyncId());
  }

  @Test
  public void testRequestBodyCanBeOmittedAltogether() throws Exception {
    api.replaceNode("demoSourceNode", null);
    verify(mockCommandRunner).runCollectionCommand(any(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(1, createdMessageProps.size());
    assertEquals("demoSourceNode", createdMessageProps.get("sourceNode"));
  }

  @Test
  public void testOptionalValuesNotAddedToRemoteMessageIfNotProvided() throws Exception {
    final var requestBody = new ReplaceNodeRequestBody("demoTargetNode", null, null);
    api.replaceNode("demoSourceNode", requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();

    assertEquals(2, createdMessageProps.size());
    assertEquals("demoSourceNode", createdMessageProps.get("sourceNode"));
    assertEquals("demoTargetNode", createdMessageProps.get("targetNode"));
    assertFalse(
        "Expected message to not contain value for waitForFinalState: "
            + createdMessageProps.get("waitForFinalState"),
        createdMessageProps.containsKey("waitForFinalState"));
    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.REPLACENODE, context.getAction());
    assertNull("asyncId should be null", context.getAsyncId());
  }
}

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

import static org.mockito.Mockito.when;

import org.apache.solr.client.api.model.ReplaceNodeRequestBody;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link ReplaceNode} */
public class ReplaceNodeAPITest extends MockV2APITest {

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

    validateRunCommand(
        CollectionParams.CollectionAction.REPLACENODE,
        requestBody.async,
        message -> {
          assertEquals(3, message.size());
          assertEquals("demoSourceNode", message.get("sourceNode"));
          assertEquals("demoTargetNode", message.get("targetNode"));
          assertEquals(false, message.get("waitForFinalState"));
        });
  }

  @Test
  public void testRequestBodyCanBeOmittedAltogether() throws Exception {
    api.replaceNode("demoSourceNode", null);

    validateRunCommand(
        CollectionParams.CollectionAction.REPLACENODE,
        message -> {
          assertEquals(1, message.size());
          assertEquals("demoSourceNode", message.get("sourceNode"));
        });
  }

  @Test
  public void testOptionalValuesNotAddedToRemoteMessageIfNotProvided() throws Exception {
    final var requestBody = new ReplaceNodeRequestBody("demoTargetNode", null, null);

    api.replaceNode("demoSourceNode", requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.REPLACENODE,
        message -> {
          assertEquals(2, message.size());
          assertEquals("demoSourceNode", message.get("sourceNode"));
          assertEquals("demoTargetNode", message.get("targetNode"));
          assertFalse(
              "Expected message to not contain value for waitForFinalState: "
                  + message.get("waitForFinalState"),
              message.containsKey("waitForFinalState"));
        });
  }
}

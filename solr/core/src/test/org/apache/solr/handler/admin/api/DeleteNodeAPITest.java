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

import static org.mockito.Mockito.mock;

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.DeleteNodeRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link DeleteNode} */
public class DeleteNodeAPITest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testV1InvocationThrowsErrorsIfRequiredParametersMissing() {
    final var api = mock(DeleteNode.class);
    final SolrException e =
        expectThrows(
            SolrException.class,
            () -> {
              DeleteNode.invokeUsingV1Inputs(api, new ModifiableSolrParams());
            });
    assertEquals("Missing required parameter: node", e.getMessage());
  }

  @Test
  public void testValidOverseerMessageIsCreated() {
    final var requestBody = new DeleteNodeRequestBody();
    requestBody.async = "async";
    final ZkNodeProps createdMessage =
        DeleteNode.createRemoteMessage("nodeNameToDelete", requestBody);
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(3, createdMessageProps.size());
    assertEquals("nodeNameToDelete", createdMessageProps.get("node"));
    assertEquals("async", createdMessageProps.get("async"));
    assertEquals("deletenode", createdMessageProps.get("operation"));
  }

  @Test
  public void testRequestBodyCanBeOmitted() throws Exception {
    final ZkNodeProps createdMessage = DeleteNode.createRemoteMessage("nodeNameToDelete", null);
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(2, createdMessageProps.size());
    assertEquals("nodeNameToDelete", createdMessageProps.get("node"));
    assertEquals("deletenode", createdMessageProps.get("operation"));
    assertFalse(
        "Expected message to not contain value for async: " + createdMessageProps.get("async"),
        createdMessageProps.containsKey("async"));
  }
}

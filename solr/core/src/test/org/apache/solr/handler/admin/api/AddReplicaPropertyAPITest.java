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

import java.util.Map;
import org.apache.solr.client.api.model.AddReplicaPropertyRequestBody;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link AddReplicaProperty} */
public class AddReplicaPropertyAPITest extends MockAPITest {

  private static final AddReplicaPropertyRequestBody ANY_REQ_BODY =
      new AddReplicaPropertyRequestBody("anyValue");

  private AddReplicaProperty addReplicaPropApi;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    addReplicaPropApi = new AddReplicaProperty(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorWhenCalledInStandaloneMode() {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(false);

    final SolrException e =
        expectThrows(
            SolrException.class,
            () -> {
              addReplicaPropApi.addReplicaProperty(
                  "someColl", "someShard", "someReplica", "somePropName", ANY_REQ_BODY);
            });
    assertEquals(400, e.code());
    assertTrue(
        "Exception message differed from expected: " + e.getMessage(),
        e.getMessage().contains("not running in SolrCloud mode"));
  }

  @Test
  public void testCreatesValidOverseerMessage() throws Exception {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    addReplicaPropApi.addReplicaProperty(
        "someColl", "someShard", "someReplica", "somePropName", ANY_REQ_BODY);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(5, createdMessageProps.size());
    assertEquals("someColl", createdMessageProps.get("collection"));
    assertEquals("someShard", createdMessageProps.get("shard"));
    assertEquals("someReplica", createdMessageProps.get("replica"));
    assertEquals("somePropName", createdMessageProps.get("property"));
    assertEquals("anyValue", createdMessageProps.get("property.value"));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.ADDREPLICAPROP, context.getAction());
    assertNull("asyncId should be null", context.getAsyncId());
  }
}

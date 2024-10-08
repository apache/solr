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

import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SHARD_UNIQUE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentracing.noop.NoopSpan;
import java.util.Map;
import java.util.Optional;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.AddReplicaPropertyRequestBody;
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

/** Unit tests for {@link AddReplicaProperty} */
public class AddReplicaPropertyAPITest extends SolrTestCaseJ4 {

  private static final AddReplicaPropertyRequestBody ANY_REQ_BODY =
      new AddReplicaPropertyRequestBody("anyValue");

  private CoreContainer mockCoreContainer;
  private DistributedCollectionConfigSetCommandRunner mockCommandRunner;
  private SolrQueryRequest mockQueryRequest;
  private SolrQueryResponse queryResponse;
  private ArgumentCaptor<ZkNodeProps> messageCapturer;

  private AddReplicaProperty addReplicaPropApi;

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
    when(mockQueryRequest.getSpan()).thenReturn(NoopSpan.INSTANCE);
    queryResponse = new SolrQueryResponse();
    messageCapturer = ArgumentCaptor.forClass(ZkNodeProps.class);

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
    verify(mockCommandRunner).runCollectionCommand(messageCapturer.capture(), any(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> createdMessageProps = createdMessage.getProperties();
    assertEquals(6, createdMessageProps.size());
    assertEquals("addreplicaprop", createdMessageProps.get("operation"));
    assertEquals("someColl", createdMessageProps.get("collection"));
    assertEquals("someShard", createdMessageProps.get("shard"));
    assertEquals("someReplica", createdMessageProps.get("replica"));
    assertEquals("somePropName", createdMessageProps.get("property"));
    assertEquals("anyValue", createdMessageProps.get("property.value"));
    assertFalse(
        createdMessageProps.containsKey(
            SHARD_UNIQUE)); // Omitted since not specified on request body
  }
}

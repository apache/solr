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
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.Span;
import java.util.Optional;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;

/**
 * Abstract test class to setup shared mocks for unit testing v2 API calls that go to the Overseer or the DistributedCollectionConfigSetCommandRunner.
 */
public abstract class MockAPITest extends SolrTestCaseJ4 {

  protected CoreContainer mockCoreContainer;
  protected ZkController mockZkController;
  protected ClusterState mockClusterState;
  protected DistributedCollectionConfigSetCommandRunner mockCommandRunner;
  protected SolrZkClient mockSolrZkClient;
  protected ZkStateReader mockZkStateReader;
  protected SolrQueryRequest mockQueryRequest;
  protected SolrQueryResponse queryResponse;
  protected ArgumentCaptor<ZkNodeProps> messageCapturer;
  protected ArgumentCaptor<AdminCmdContext> contextCapturer;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    mockCoreContainer = mock(CoreContainer.class);
    mockZkController = mock(ZkController.class);
    mockClusterState = mock(ClusterState.class);
    mockSolrZkClient = mock(SolrZkClient.class);
    mockZkStateReader = mock(ZkStateReader.class);
    mockCommandRunner = mock(DistributedCollectionConfigSetCommandRunner.class);
    when(mockCoreContainer.getZkController()).thenReturn(mockZkController);
    when(mockCoreContainer.getAliases()).thenReturn(Aliases.EMPTY);
    when(mockZkController.getDistributedCommandRunner()).thenReturn(Optional.of(mockCommandRunner));
    when(mockZkController.getClusterState()).thenReturn(mockClusterState);
    when(mockZkController.getZkStateReader()).thenReturn(mockZkStateReader);
    when(mockZkController.getZkClient()).thenReturn(mockSolrZkClient);
    when(mockCommandRunner.runCollectionCommand(any(), any(), anyLong()))
        .thenReturn(new OverseerSolrResponse(new NamedList<>()));
    mockQueryRequest = mock(SolrQueryRequest.class);
    when(mockQueryRequest.getSpan()).thenReturn(Span.getInvalid());
    queryResponse = new SolrQueryResponse();
    messageCapturer = ArgumentCaptor.forClass(ZkNodeProps.class);
    contextCapturer = ArgumentCaptor.forClass(AdminCmdContext.class);
  }
}

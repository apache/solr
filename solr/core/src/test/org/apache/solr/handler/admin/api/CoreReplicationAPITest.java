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
import static org.mockito.Mockito.when;

import io.opentracing.noop.NoopSpan;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link CoreReplicationAPI} */
public class CoreReplicationAPITest extends SolrTestCaseJ4 {

  private CoreReplicationAPI coreReplicationAPI;
  private SolrCore mockCore;
  private ReplicationHandler mockReplicationHandler;
  private static final String coreName = "test";
  private SolrQueryRequest mockQueryRequest;
  private SolrQueryResponse queryResponse;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    setUpMocks();
    mockQueryRequest = mock(SolrQueryRequest.class);
    when(mockQueryRequest.getSpan()).thenReturn(NoopSpan.INSTANCE);
    queryResponse = new SolrQueryResponse();
    coreReplicationAPI = new CoreReplicationAPI(mockCore, mockQueryRequest, queryResponse);
  }

  @Test
  public void testGetIndexVersion() throws Exception {
    CoreReplicationAPI.IndexVersionResponse expected =
        new CoreReplicationAPI.IndexVersionResponse(123L, 123L, "testGeneration");
    when(mockReplicationHandler.getIndexVersionResponse()).thenReturn(expected);

    CoreReplicationAPI.IndexVersionResponse response = coreReplicationAPI.doFetchIndexVersion();
    assertEquals(expected.indexVersion, response.indexVersion);
    assertEquals(expected.generation, response.generation);
    assertEquals(expected.status, response.status);
  }

  private void setUpMocks() {
    mockCore = mock(SolrCore.class);
    mockReplicationHandler = mock(ReplicationHandler.class);
    when(mockCore.getRequestHandler(ReplicationHandler.PATH)).thenReturn(mockReplicationHandler);
  }
}

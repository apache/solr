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

import static org.apache.solr.handler.ReplicationHandler.CMD_GET_FILE_LIST;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentracing.noop.NoopSpan;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
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
    coreReplicationAPI = new CoreReplicationAPIMock(mockCore, mockQueryRequest, queryResponse);
  }

  @Test
  public void testGetIndexVersion() throws Exception {
    CoreReplicationAPI.IndexVersionResponse expected =
        new CoreReplicationAPI.IndexVersionResponse(123L, 123L, "testGeneration");
    when(mockReplicationHandler.getIndexVersionResponse()).thenReturn(expected);

    CoreReplicationAPI.IndexVersionResponse actual = coreReplicationAPI.doFetchIndexVersion();
    assertEquals(expected.indexVersion, actual.indexVersion);
    assertEquals(expected.generation, actual.generation);
    assertEquals(expected.status, actual.status);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFetchFiles() throws Exception {
    NamedList<Object> actualResponse = coreReplicationAPI.fetchFiles(-1);
    assertEquals("filelist", actualResponse.getName(0));
    Map<String, Object> actual = (Map<String, Object>) actualResponse.get("filelist");
    assertEquals(123, actual.get("size"));
    assertEquals("test", actual.get("name"));
    assertEquals(123456789, actual.get("checksum"));
  }

  private void setUpMocks() {
    mockCore = mock(SolrCore.class);
    mockReplicationHandler = mock(ReplicationHandler.class);
    when(mockCore.getRequestHandler(ReplicationHandler.PATH)).thenReturn(mockReplicationHandler);
  }

  class CoreReplicationAPIMock extends CoreReplicationAPI {
    public CoreReplicationAPIMock(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
      super(solrCore, req, rsp);
    }

    @Override
    protected NamedList<Object> getFileList(
        long generation, ReplicationHandler replicationHandler) {
      final NamedList<Object> filesResponse = new NamedList<>();
      filesResponse.add(
          CMD_GET_FILE_LIST, Map.of("size", 123, "name", "test", "checksum", 123456789));
      return filesResponse;
    }
  }
}

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

import io.opentelemetry.api.trace.Span;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.ReplicationFileListResponse;
import org.apache.solr.client.api.model.ReplicationFileResponse;
import org.apache.solr.client.api.model.ReplicationIndexVersionResponse;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link CoreReplication} */
public class CoreReplicationTest extends SolrTestCaseJ4 {

  private CoreReplication coreReplication;

  private SolrCore mockCore;
  private ReplicationHandler mockReplicationHandler;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    setUpMocks();
    SolrQueryRequest mockQueryRequest = mock(SolrQueryRequest.class);
    when(mockQueryRequest.getSpan()).thenReturn(Span.getInvalid());
    SolrQueryResponse queryResponse = new SolrQueryResponse();
    coreReplication = new CoreReplicationMock(mockCore, mockQueryRequest, queryResponse);
  }

  @Test
  public void testGetIndexVersion() throws Exception {
    ReplicationIndexVersionResponse expected =
        new ReplicationIndexVersionResponse(123L, 123L, "testGeneration");
    when(mockReplicationHandler.getIndexVersionResponse()).thenReturn(expected);

    ReplicationIndexVersionResponse actual = coreReplication.doFetchIndexVersion();
    assertEquals(expected.indexVersion, actual.indexVersion);
    assertEquals(expected.generation, actual.generation);
    assertEquals(expected.status, actual.status);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFetchFiles() throws Exception {
    ReplicationFileListResponse actualResponse =
        (ReplicationFileListResponse) coreReplication.fetchFileList(mockCore.getName(), -1);
    assertEquals(123, actualResponse.fileList.get(0).size);
    assertEquals("test", actualResponse.fileList.get(0).name);
    assertEquals(123456789, actualResponse.fileList.get(0).checksum);
  }

  @Test
  public void testFetchFileStreams() throws Exception {
    ReplicationFileResponse actual =
        coreReplication.doFetchFile("./test", "file", null, null, false, false, 0, null);
    assertTrue(actual.dfs instanceof ReplicationAPIBase.DirectoryFileStream);

    actual = coreReplication.doFetchFile("./test", "tlogFile", null, null, false, false, 0, null);
    assertTrue(actual.dfs instanceof ReplicationAPIBase.LocalFsTlogFileStream);

    actual = coreReplication.doFetchFile("./test", "cf", null, null, false, false, 0, null);
    assertTrue(actual.dfs instanceof ReplicationAPIBase.LocalFsConfFileStream);
  }

  private void setUpMocks() throws IOException {
    mockCore = mock(SolrCore.class);
    mockReplicationHandler = mock(ReplicationHandler.class);

    // Mocks for LocalFsTlogFileStream
    UpdateHandler mockUpdateHandler = mock(UpdateHandler.class);
    UpdateLog mockUpdateLog = mock(UpdateLog.class);
    when(mockUpdateHandler.getUpdateLog()).thenReturn(mockUpdateLog);
    when(mockUpdateLog.getTlogDir()).thenReturn("ignore");

    // Mocks for LocalFsConfFileStream
    SolrResourceLoader mockSolrResourceLoader = mock(SolrResourceLoader.class);
    Path mockPath = mock(Path.class);
    when(mockCore.getRequestHandler(ReplicationHandler.PATH)).thenReturn(mockReplicationHandler);
    when(mockCore.getUpdateHandler()).thenReturn(mockUpdateHandler);
    when(mockCore.getResourceLoader()).thenReturn(mockSolrResourceLoader);
    when(mockSolrResourceLoader.getConfigPath()).thenReturn(mockPath);
  }

  private static class CoreReplicationMock extends CoreReplication {
    public CoreReplicationMock(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
      super(solrCore, req, rsp);
    }

    @Override
    protected ReplicationFileListResponse getFileList(
        long generation, ReplicationHandler replicationHandler) {
      final ReplicationFileListResponse filesResponse = new ReplicationFileListResponse();
      List<ReplicationFileListResponse.FileMetaData> fileMetaData =
          Arrays.asList(new ReplicationFileListResponse.FileMetaData(123, "test", 123456789));
      filesResponse.fileList = new ArrayList<>(fileMetaData);
      return filesResponse;
    }
  }
}

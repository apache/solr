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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  private SolrQueryRequest mockQueryRequest;
  private SolrQueryResponse queryResponse;
  private ReplicationAPIBase.DirectoryFileStream mockDirectoryFileStream;
  private OutputStream outputStream;

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
    when(mockQueryRequest.getSpan()).thenReturn(Span.getInvalid());
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
    CoreReplicationAPI.FileListResponse actualResponse = coreReplicationAPI.fetchFileList(-1);
    assertEquals(123, actualResponse.fileList.get(0).size);
    assertEquals("test", actualResponse.fileList.get(0).name);
    assertEquals(123456789, actualResponse.fileList.get(0).checksum);
  }

  @Test
  public void testFetchFile() throws Exception {
    String expected = "Random output stream data";
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    String actual =
        coreReplicationAPI.doFetchFile(
            "_0_Lucene99_0.tmd", "file", null, null, false, false, 0, null);
    assertEquals(expected, actual);
  }

  private void setUpMocks() throws IOException {
    mockCore = mock(SolrCore.class);
    mockReplicationHandler = mock(ReplicationHandler.class);
    when(mockCore.getRequestHandler(ReplicationHandler.PATH)).thenReturn(mockReplicationHandler);
  }

  private static class CoreReplicationAPIMock extends CoreReplicationAPI {
    public CoreReplicationAPIMock(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
      super(solrCore, req, rsp);
    }

    @Override
    protected FileListResponse getFileList(long generation, ReplicationHandler replicationHandler) {
      final FileListResponse filesResponse = new FileListResponse();
      List<FileMetaData> fileMetaData = Arrays.asList(new FileMetaData(123, "test", 123456789));
      filesResponse.fileList = new ArrayList<>(fileMetaData);
      return filesResponse;
    }

    @Override
    protected String getFile(DirectoryFileStream dfs, ByteArrayOutputStream out)
        throws IOException {
      String mockOutputStream = "Random output stream data";
      out.write(mockOutputStream.getBytes(StandardCharsets.UTF_8));
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }
}

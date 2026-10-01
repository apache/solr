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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.Constants;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.FileListResponse;
import org.apache.solr.client.api.model.FileMetaData;
import org.apache.solr.client.api.model.IndexVersionResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link CoreReplication} */
public class CoreReplicationAPITest extends SolrTestCaseJ4 {

  private CoreReplication coreReplicationAPI;
  private SolrCore mockCore;
  private ReplicationHandler mockReplicationHandler;
  private Path configPath;
  private Path tlogDir;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    setUpMocks();
    final var mockQueryRequest = mock(SolrQueryRequest.class);
    when(mockQueryRequest.getSpan()).thenReturn(Span.getInvalid());
    final var queryResponse = new SolrQueryResponse();
    coreReplicationAPI = new CoreReplicationAPIMock(mockCore, mockQueryRequest, queryResponse);
  }

  @Test
  public void testGetIndexVersion() throws Exception {
    IndexVersionResponse expected = new IndexVersionResponse(123L, 123L, "testGeneration");
    when(mockReplicationHandler.getIndexVersionResponse()).thenReturn(expected);

    IndexVersionResponse actual = coreReplicationAPI.doFetchIndexVersion();
    assertEquals(expected.indexVersion, actual.indexVersion);
    assertEquals(expected.generation, actual.generation);
    assertEquals(expected.status, actual.status);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFetchFiles() throws Exception {
    FileListResponse actualResponse = coreReplicationAPI.fetchFileList(-1);
    assertEquals(123, actualResponse.fileList.get(0).size);
    assertEquals("test", actualResponse.fileList.get(0).name);
    assertEquals(123456789, actualResponse.fileList.get(0).checksum);
  }

  @Test
  public void testFetchFile() throws Exception {
    ReplicationAPIBase.DirectoryFileStream actual =
        coreReplicationAPI.doFetchFile("./test", "file", null, null, false, false, 0, null);
    assertNotNull(actual);

    actual =
        coreReplicationAPI.doFetchFile("./test", "tlogFile", null, null, false, false, 0, null);
    assertTrue(actual instanceof ReplicationAPIBase.LocalFsTlogFileStream);

    actual = coreReplicationAPI.doFetchFile("./test", "cf", null, null, false, false, 0, null);
    assertTrue(actual instanceof ReplicationAPIBase.LocalFsConfFileStream);
  }

  @Test
  public void testValidateFilenameRejectsBackslashTraversal() {
    // Backslash is a separator for validation on every platform, not just Windows.
    for (String dirType : new String[] {"cf", "tlogFile"}) {
      SolrException ex =
          expectThrows(
              SolrException.class,
              "Expected FORBIDDEN for dirType=" + dirType,
              () ->
                  coreReplicationAPI.doFetchFile(
                      "\\..\\conf\\solrconfig.xml", dirType, null, null, false, false, 0, null));
      assertEquals(
          "Wrong error code for dirType=" + dirType,
          SolrException.ErrorCode.FORBIDDEN.code,
          ex.code());
    }
  }

  @Test
  public void testValidateFilenameRejectsTraversalVariants() {
    // Mixed separators, across both conf-file and tlog-file dirTypes.
    String[] inputs = {
      "\\..\\conf\\solrconfig.xml",
      "/..\\conf\\solrconfig.xml",
      "\\../conf/solrconfig.xml",
      "foo\\..\\..\\other\\file.txt",
      "..\\other\\file.txt",
    };
    for (String dirType : new String[] {"cf", "tlogFile"}) {
      for (String input : inputs) {
        SolrException ex =
            expectThrows(
                SolrException.class,
                "Expected FORBIDDEN for dirType=" + dirType + " input=" + input,
                () ->
                    coreReplicationAPI.doFetchFile(
                        input, dirType, null, null, false, false, 0, null));
        assertEquals(
            "Wrong error code for dirType=" + dirType + " input=" + input,
            SolrException.ErrorCode.FORBIDDEN.code,
            ex.code());
      }
    }
  }

  @Test
  public void testValidateFilenameRejectsAbsolutePath() {
    // On POSIX, '/abs/file.txt' is absolute; validateFilenameOrError must reject it
    // via its isAbsolute() branch. Windows treats a leading '/' as drive-relative
    // (not absolute), so on Windows the containment check in initFile carries the
    // load instead; this assertion targets the POSIX code path explicitly.
    Assume.assumeFalse(Constants.WINDOWS);
    SolrException ex =
        expectThrows(
            SolrException.class,
            () ->
                coreReplicationAPI.doFetchFile(
                    "/abs/file.txt", "cf", null, null, false, false, 0, null));
    assertEquals(SolrException.ErrorCode.FORBIDDEN.code, ex.code());
  }

  @Test
  public void testValidateFilenameRejectsWindowsAbsolutePath() {
    // Symmetric to testValidateFilenameRejectsAbsolutePath: on Windows,
    // 'C:\Windows\...' IS absolute and must be rejected by
    // validateFilenameOrError's isAbsolute() branch. POSIX treats 'C:\...' as a
    // relative path with a literal ':' character, so Path.of won't mark it
    // absolute there; this assertion is therefore gated to Windows only.
    Assume.assumeTrue(Constants.WINDOWS);
    SolrException ex =
        expectThrows(
            SolrException.class,
            () ->
                coreReplicationAPI.doFetchFile(
                    "C:\\Windows\\System32\\drivers\\etc\\hosts",
                    "cf",
                    null,
                    null,
                    false,
                    false,
                    0,
                    null));
    assertEquals(SolrException.ErrorCode.FORBIDDEN.code, ex.code());
  }

  @Test
  public void testValidateFilenameRejectsInvalidPath() {
    // A NUL character is rejected by Path.of on all platforms; ensure the
    // InvalidPathException is translated into a BAD_REQUEST SolrException
    // (malformed input, not access-denied) rather than bubbling as an
    // unhandled runtime error.
    SolrException ex =
        expectThrows(
            SolrException.class,
            () ->
                coreReplicationAPI.doFetchFile(
                    "foo\u0000bar", "cf", null, null, false, false, 0, null));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
  }

  @Test
  public void testValidNestedPathAccepted() throws IOException {
    // Positive test: legal nested relative paths (e.g. 'lang/en.txt') must not
    // be rejected by validateFilenameOrError OR by the isPathAChildOfParent
    // containment check in LocalFs{Conf,Tlog}FileStream.initFile(). Guards
    // against those checks over-rejecting.
    // Also exercises the normalize() applied to the resolved path by
    // resolveWithinOrForbidden; the on-disk file must still be readable.
    Files.createDirectories(configPath.resolve("lang"));
    Path cfFile = configPath.resolve("lang").resolve("en.txt");
    Files.writeString(cfFile, "hello");
    Files.createDirectories(tlogDir.resolve("nested"));
    Path tlogFile = tlogDir.resolve("nested").resolve("tlog.0");
    Files.writeString(tlogFile, "hello");

    ReplicationAPIBase.DirectoryFileStream cfStream =
        coreReplicationAPI.doFetchFile("lang/en.txt", "cf", null, null, false, false, 0, null);
    assertTrue(cfStream instanceof ReplicationAPIBase.LocalFsConfFileStream);
    assertTrue("Expected readable config file at " + cfFile, Files.isReadable(cfFile));
    assertEquals(cfFile.normalize(), configPath.resolve("lang/en.txt").normalize());

    ReplicationAPIBase.DirectoryFileStream tlogStream =
        coreReplicationAPI.doFetchFile(
            "nested/tlog.0", "tlogFile", null, null, false, false, 0, null);
    assertTrue(tlogStream instanceof ReplicationAPIBase.LocalFsTlogFileStream);
    assertTrue("Expected readable tlog file at " + tlogFile, Files.isReadable(tlogFile));
    assertEquals(tlogFile.normalize(), tlogDir.resolve("nested/tlog.0").normalize());
  }

  @Test
  public void testDoFetchFileRejectsNullFilename() {
    // A null file argument for the LocalFs{Conf,Tlog}FileStream paths
    // must surface as a clean 4xx SolrException rather than an NPE bubbling out
    // of Path.resolve(null). Guards the null-child branch of
    // resolveWithinOrForbidden for both dirType=cf and dirType=tlogFile.
    for (String dirType : new String[] {"cf", "tlogFile"}) {
      SolrException ex =
          expectThrows(
              SolrException.class,
              "Expected 4xx for dirType=" + dirType,
              () ->
                  coreReplicationAPI.doFetchFile(null, dirType, null, null, false, false, 0, null));
      assertTrue(
          "Expected a 4xx error code for dirType=" + dirType + ", got " + ex.code(),
          ex.code() >= 400 && ex.code() < 500);
    }
  }

  @Test
  public void testInitFileGuardCatchesPathEscapingConfigDir() {
    // Directly exercise LocalFsConfFileStream.initFile()'s isPathAChildOfParent guard,
    // independent of validateFilenameOrError.
    SolrException ex =
        expectThrows(
            SolrException.class,
            () ->
                ((CoreReplicationAPIMock) coreReplicationAPI)
                    .newUnvalidatedConfFileStream("../../other/file.txt"));
    assertEquals(SolrException.ErrorCode.FORBIDDEN.code, ex.code());
  }

  @Test
  public void testInitFileGuardCatchesPathEscapingTlogDir() {
    // Symmetric to testInitFileGuardCatchesPathEscapingConfigDir: ensures the
    // LocalFsTlogFileStream.initFile() containment check rejects paths that
    // resolve outside the tlog directory, independent of validateFilenameOrError.
    SolrException ex =
        expectThrows(
            SolrException.class,
            () ->
                ((CoreReplicationAPIMock) coreReplicationAPI)
                    .newUnvalidatedTlogFileStream("../../other/file.txt"));
    assertEquals(SolrException.ErrorCode.FORBIDDEN.code, ex.code());
  }

  private void setUpMocks() throws IOException {
    mockCore = mock(SolrCore.class);
    mockReplicationHandler = mock(ReplicationHandler.class);

    // Mocks for LocalFsTlogFileStream
    UpdateHandler mockUpdateHandler = mock(UpdateHandler.class);
    UpdateLog mockUpdateLog = mock(UpdateLog.class);
    tlogDir = createTempDir("coreReplicationTlog");
    when(mockUpdateHandler.getUpdateLog()).thenReturn(mockUpdateLog);
    when(mockUpdateLog.getTlogDir()).thenReturn(tlogDir.toString());

    // Mocks for LocalFsConfFileStream
    SolrResourceLoader mockSolrResourceLoader = mock(SolrResourceLoader.class);
    configPath = createTempDir("coreReplicationConf");
    when(mockCore.getRequestHandler(ReplicationHandler.PATH)).thenReturn(mockReplicationHandler);
    when(mockCore.getUpdateHandler()).thenReturn(mockUpdateHandler);
    when(mockCore.getResourceLoader()).thenReturn(mockSolrResourceLoader);
    when(mockSolrResourceLoader.getConfigPath()).thenReturn(configPath);
  }

  private static class CoreReplicationAPIMock extends CoreReplication {
    public CoreReplicationAPIMock(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
      super(solrCore, req, rsp);
    }

    @Override
    protected FileListResponse getFileList(long generation, ReplicationHandler replicationHandler) {
      final var filesResponse = new FileListResponse();
      List<FileMetaData> fileMetaData = Arrays.asList(new FileMetaData(123, "test", 123456789));
      filesResponse.fileList = new ArrayList<>(fileMetaData);
      return filesResponse;
    }

    ReplicationAPIBase.LocalFsConfFileStream newUnvalidatedConfFileStream(String cfname) {
      return new LocalFsConfFileStream(cfname, "cf", null, null, false, false, 0, null) {
        @Override
        protected String validateFilenameOrError(String fileName) {
          return fileName;
        }
      };
    }

    ReplicationAPIBase.LocalFsTlogFileStream newUnvalidatedTlogFileStream(String tlogname) {
      return new LocalFsTlogFileStream(tlogname, "tlogFile", null, null, false, false, 0, null) {
        @Override
        protected String validateFilenameOrError(String fileName) {
          return fileName;
        }
      };
    }
  }
}

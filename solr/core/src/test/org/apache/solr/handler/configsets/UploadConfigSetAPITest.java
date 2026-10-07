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

package org.apache.solr.handler.configsets;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.FileSystemConfigSetService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link UploadConfigSet#uploadConfigSet} (Upload interface). */
public class UploadConfigSetAPITest extends SolrTestCase {

  private CoreContainer mockCoreContainer;
  private FileSystemConfigSetService configSetService;
  private Path configSetBase;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void initConfigSetService() {
    configSetBase = createTempDir("configsets");
    // Use an anonymous subclass to access the protected testing constructor
    configSetService = new FileSystemConfigSetService(configSetBase) {};
    mockCoreContainer = mock(CoreContainer.class);
    when(mockCoreContainer.getConfigSetService()).thenReturn(configSetService);
  }

  /** Creates an in-memory ZIP file with the specified files. */
  @SuppressWarnings("try") // ZipOutputStream must be closed to finalize ZIP format
  private InputStream createZipStream(String... filePathAndContent) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      for (int i = 0; i < filePathAndContent.length; i += 2) {
        String filePath = filePathAndContent[i];
        String content = filePathAndContent[i + 1];
        zos.putNextEntry(new ZipEntry(filePath));
        zos.write(content.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
      }
    }
    return new ByteArrayInputStream(baos.toByteArray());
  }

  /** Creates an empty ZIP file. */
  @SuppressWarnings("try") // ZipOutputStream must be closed even with no entries
  private InputStream createEmptyZipStream() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      // No entries
    }
    return new ByteArrayInputStream(baos.toByteArray());
  }

  /** Creates a configset with files on disk for testing overwrites and cleanup. */
  private void createExistingConfigSet(String configSetName, String... filePathAndContent)
      throws Exception {
    Path configDir = configSetBase.resolve(configSetName);
    Files.createDirectories(configDir);
    for (int i = 0; i < filePathAndContent.length; i += 2) {
      String filePath = filePathAndContent[i];
      String content = filePathAndContent[i + 1];
      Path fullPath = configDir.resolve(filePath);
      Files.createDirectories(fullPath.getParent());
      Files.writeString(fullPath, content, StandardCharsets.UTF_8);
    }
  }

  @Test
  public void testSuccessfulZipUpload() throws Exception {
    final String configSetName = "newconfig";
    InputStream zipStream = createZipStream("solrconfig.xml", "<config/>");

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    final var response = api.uploadConfigSet(configSetName, true, false, zipStream);

    assertNotNull(response);
    assertTrue(
        "ConfigSet should exist after upload", configSetService.checkConfigExists(configSetName));

    // Verify the file was uploaded
    byte[] uploadedData = configSetService.downloadFileFromConfig(configSetName, "solrconfig.xml");
    assertEquals("<config/>", new String(uploadedData, StandardCharsets.UTF_8));
  }

  @Test
  public void testSuccessfulZipUploadWithMultipleFiles() throws Exception {
    final String configSetName = "multifile";
    InputStream zipStream =
        createZipStream(
            "solrconfig.xml", "<config/>",
            "schema.xml", "<schema/>",
            "stopwords.txt", "a\nthe");

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    final var response = api.uploadConfigSet(configSetName, true, false, zipStream);

    assertNotNull(response);
    assertTrue(configSetService.checkConfigExists(configSetName));

    // Verify all files were uploaded
    byte[] solrconfig = configSetService.downloadFileFromConfig(configSetName, "solrconfig.xml");
    assertEquals("<config/>", new String(solrconfig, StandardCharsets.UTF_8));

    byte[] schema = configSetService.downloadFileFromConfig(configSetName, "schema.xml");
    assertEquals("<schema/>", new String(schema, StandardCharsets.UTF_8));

    byte[] stopwords = configSetService.downloadFileFromConfig(configSetName, "stopwords.txt");
    assertEquals("a\nthe", new String(stopwords, StandardCharsets.UTF_8));
  }

  @Test
  public void testEmptyZipThrowsBadRequest() throws Exception {
    try (InputStream emptyZip = createEmptyZipStream()) {

      final var api = new UploadConfigSet(mockCoreContainer, null, null);
      final var ex =
          assertThrows(
              SolrException.class, () -> api.uploadConfigSet("newconfig", true, false, emptyZip));

      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(
          "Error message should mention empty zip",
          ex.getMessage().contains("empty zipped data") || ex.getMessage().contains("non-zipped"));
    }
  }

  @Test
  public void testNonZipDataThrowsBadRequest() {
    // Send plain text instead of a ZIP
    InputStream notAZip =
        new ByteArrayInputStream("this is not a zip file".getBytes(StandardCharsets.UTF_8));

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    // This should fail either as bad ZIP or as empty ZIP
    assertThrows(Exception.class, () -> api.uploadConfigSet("newconfig", true, false, notAZip));
  }

  @Test
  public void testOverwriteExistingConfigSet() throws Exception {
    final String configSetName = "existing";
    // Create existing configset with old content
    createExistingConfigSet(configSetName, "solrconfig.xml", "<old-config/>");

    // Upload new content with overwrite=true
    InputStream zipStream = createZipStream("solrconfig.xml", "<new-config/>");
    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    final var response = api.uploadConfigSet(configSetName, true, false, zipStream);

    assertNotNull(response);

    // Verify the file was overwritten
    byte[] uploadedData = configSetService.downloadFileFromConfig(configSetName, "solrconfig.xml");
    assertEquals("<new-config/>", new String(uploadedData, StandardCharsets.UTF_8));
  }

  @Test
  public void testOverwriteFalseThrowsExceptionWhenExists() throws Exception {
    final String configSetName = "existing";
    createExistingConfigSet(configSetName, "solrconfig.xml", "<old-config/>");

    try (InputStream zipStream = createZipStream("solrconfig.xml", "<new-config/>")) {
      final var api = new UploadConfigSet(mockCoreContainer, null, null);

      final var ex =
          assertThrows(
              SolrException.class,
              () -> api.uploadConfigSet(configSetName, false, false, zipStream));

      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(
          "Error message should mention config already exists",
          ex.getMessage().contains("already"));
    }
  }

  @Test
  public void testCleanupRemovesUnusedFiles() throws Exception {
    final String configSetName = "cleanuptest";
    // Create existing configset with multiple files
    createExistingConfigSet(
        configSetName,
        "solrconfig.xml",
        "<old-config/>",
        "schema.xml",
        "<old-schema/>",
        "old-file.txt",
        "to be deleted");

    // Upload new ZIP with only one file and cleanup=true
    InputStream zipStream = createZipStream("solrconfig.xml", "<new-config/>");
    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    final var response = api.uploadConfigSet(configSetName, true, true, zipStream);

    assertNotNull(response);

    // Verify solrconfig.xml was updated
    byte[] solrconfig = configSetService.downloadFileFromConfig(configSetName, "solrconfig.xml");
    assertEquals("<new-config/>", new String(solrconfig, StandardCharsets.UTF_8));

    // Verify old files were deleted (should throw or return null)
    try {
      byte[] oldSchema = configSetService.downloadFileFromConfig(configSetName, "schema.xml");
      if (oldSchema != null) {
        fail("schema.xml should have been deleted during cleanup");
      }
    } catch (Exception e) {
      // Expected - file should not exist
    }
  }

  @Test
  public void testCleanupFalseKeepsExistingFiles() throws Exception {
    final String configSetName = "nocleanup";
    // Create existing configset with multiple files
    createExistingConfigSet(
        configSetName, "solrconfig.xml", "<old-config/>", "schema.xml", "<old-schema/>");

    // Upload new ZIP with only one file and cleanup=false
    InputStream zipStream = createZipStream("solrconfig.xml", "<new-config/>");
    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    final var response = api.uploadConfigSet(configSetName, true, false, zipStream);

    assertNotNull(response);

    // Verify solrconfig.xml was updated
    byte[] solrconfig = configSetService.downloadFileFromConfig(configSetName, "solrconfig.xml");
    assertEquals("<new-config/>", new String(solrconfig, StandardCharsets.UTF_8));

    // Verify schema.xml still exists
    byte[] schema = configSetService.downloadFileFromConfig(configSetName, "schema.xml");
    assertEquals("<old-schema/>", new String(schema, StandardCharsets.UTF_8));
  }

  @Test
  public void testDefaultParametersWhenNull() throws Exception {
    final String configSetName = "defaults";
    InputStream zipStream = createZipStream("solrconfig.xml", "<config/>");

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    // Pass null for overwrite and cleanup - should use defaults (overwrite=true, cleanup=false)
    final var response = api.uploadConfigSet(configSetName, null, null, zipStream);

    assertNotNull(response);
    assertTrue(configSetService.checkConfigExists(configSetName));
  }

  @Test
  public void testZipWithDirectoryEntries() throws Exception {
    final String configSetName = "withdirs";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      // Add directory entry
      zos.putNextEntry(new ZipEntry("conf/"));
      zos.closeEntry();

      // Add file in directory
      zos.putNextEntry(new ZipEntry("conf/solrconfig.xml"));
      zos.write("<config/>".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    InputStream zipStream = new ByteArrayInputStream(baos.toByteArray());

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    final var response = api.uploadConfigSet(configSetName, true, false, zipStream);

    assertNotNull(response);
    assertTrue(configSetService.checkConfigExists(configSetName));

    // Directory entries should be skipped, but file should be uploaded
    byte[] uploadedData =
        configSetService.downloadFileFromConfig(configSetName, "conf/solrconfig.xml");
    assertEquals("<config/>", new String(uploadedData, StandardCharsets.UTF_8));
  }

  @Test
  public void testZipUploadNormalizesBackslashEntryNames() throws Exception {
    final String configSetName = "backslashpaths";
    createExistingConfigSet(configSetName, "lang/stopwords/old.txt", "old", "stale.txt", "stale");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry("lang\\"));
      zos.closeEntry();
      zos.putNextEntry(new ZipEntry("lang\\stopwords\\"));
      zos.closeEntry();
      zos.putNextEntry(new ZipEntry("lang\\stopwords\\en.txt"));
      zos.write("a\nthe".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    InputStream zipStream = new ByteArrayInputStream(baos.toByteArray());

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    api.uploadConfigSet(configSetName, true, true, zipStream);

    byte[] uploadedData =
        configSetService.downloadFileFromConfig(configSetName, "lang/stopwords/en.txt");
    assertEquals("a\nthe", new String(uploadedData, StandardCharsets.UTF_8));
    assertEquals(
        "lang/stopwords/en.txt", UploadConfigSet.normalizeZipEntryName("lang\\stopwords\\en.txt"));
    List<String> configFiles = configSetService.getAllConfigFiles(configSetName);
    assertTrue(configFiles.contains("lang/"));
    assertTrue(configFiles.contains("lang/stopwords/"));
    assertTrue(configFiles.contains("lang/stopwords/en.txt"));
    assertTrue(configFiles.stream().noneMatch(path -> path.contains("\\")));

    assertNull(configSetService.downloadFileFromConfig(configSetName, "lang/stopwords/old.txt"));
    assertNull(configSetService.downloadFileFromConfig(configSetName, "stale.txt"));
  }

  @Test
  public void testZipUploadRejectsPathTraversalEntries() throws Exception {
    final String configSetName = "traversalpaths";
    createExistingConfigSet(configSetName, "conf/solrconfig.xml", "<config/>");

    // The backslash entry is normalized to forward slashes before the path check,
    // so both entries below are traversal attempts on every platform. An archive
    // containing any unsafe entry path is rejected as a whole: the upload fails
    // with BAD_REQUEST and no entry, safe or not, is stored.
    for (String unsafePath : new String[] {"../evil.txt", "..\\evil.txt"}) {
      InputStream zipStream = createZipStream(unsafePath, "evil", "conf/good.txt", "good");

      final var api = new UploadConfigSet(mockCoreContainer, null, null);
      final var ex =
          assertThrows(
              SolrException.class, () -> api.uploadConfigSet(configSetName, true, true, zipStream));

      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(
          "Error message should name the offending entry", ex.getMessage().contains("../evil.txt"));
    }

    // Nothing from the rejected archives landed in the configset ...
    assertFalse(
        Files.exists(configSetBase.resolve(configSetName).resolve("conf").resolve("good.txt")));
    assertFalse(Files.exists(configSetBase.resolve(configSetName).resolve("evil.txt")));
    // ... nothing escaped next to it on disk ...
    assertFalse(Files.exists(configSetBase.resolve("evil.txt")));
    // ... and the pre-existing configset content is untouched.
    assertEquals(
        "<config/>",
        Files.readString(
            configSetBase.resolve(configSetName).resolve("conf").resolve("solrconfig.xml"),
            StandardCharsets.UTF_8));
  }

  @Test
  public void testZipUploadRejectsUnsafeEntryPathsBeforeBackendDispatch() throws Exception {
    // The traversal guard must hold for every ConfigSetService backend, not just the
    // filesystem one: the ZooKeeper backend builds a znode path from the entry name.
    // Every entry path is validated before anything is dispatched, so an archive
    // containing any unsafe entry path fails as a whole and the backend receives
    // nothing, not even the archive's safe entries. Use a mock backend and pin that
    // zero files are dispatched for each unsafe shape.
    final String configSetName = "anybackend";
    for (String unsafePath :
        new String[] {
          "../evil.txt",
          "conf/../../evil.txt",
          "..\\evil.txt",
          "/abs.txt",
          "C:/evil.txt",
          "C:\\evil.txt",
          ""
        }) {
      ConfigSetService mockService = mock(ConfigSetService.class);
      when(mockService.checkConfigExists(anyString())).thenReturn(false);
      CoreContainer container = mock(CoreContainer.class);
      when(container.getConfigSetService()).thenReturn(mockService);

      InputStream zipStream = createZipStream(unsafePath, "evil", "conf/good.txt", "good");

      final var api = new UploadConfigSet(container, null, null);
      final var ex =
          assertThrows(
              SolrException.class,
              () -> api.uploadConfigSet(configSetName, true, false, zipStream));

      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      verify(mockService, times(0)).uploadFileToConfig(anyString(), anyString(), any(), eq(true));
    }
  }

  @Test
  public void testZipUploadRejectsDriveQualifiedEntries() throws Exception {
    final String configSetName = "drivepaths";
    createExistingConfigSet(configSetName, "conf/solrconfig.xml", "<config/>");

    // Both spellings of a drive-qualified Windows path are absolute there even
    // though neither starts with "/"; the backslash form is normalized first.
    // An archive containing one is rejected as a whole.
    for (String unsafePath : new String[] {"C:/outside/evil.txt", "C:\\outside\\evil.txt"}) {
      InputStream zipStream = createZipStream(unsafePath, "evil", "conf/good.txt", "good");

      final var api = new UploadConfigSet(mockCoreContainer, null, null);
      final var ex =
          assertThrows(
              SolrException.class, () -> api.uploadConfigSet(configSetName, true, true, zipStream));

      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(
          "Error message should name the offending entry",
          ex.getMessage().contains("C:/outside/evil.txt"));
    }

    // Nothing from the rejected archives landed in the configset ...
    assertFalse(
        Files.exists(configSetBase.resolve(configSetName).resolve("conf").resolve("good.txt")));
    // ... nor was anything written under a literal "C:" directory on this platform.
    assertFalse(Files.exists(configSetBase.resolve(configSetName).resolve("C:")));
  }

  @Test
  public void testSafeZipEntryPathRules() {
    assertFalse(UploadConfigSet.isSafeZipEntryPath(""));
    assertFalse(UploadConfigSet.isSafeZipEntryPath("/abs.txt"));
    assertFalse(UploadConfigSet.isSafeZipEntryPath("C:/outside/evil.txt"));
    assertFalse(
        UploadConfigSet.isSafeZipEntryPath(
            UploadConfigSet.normalizeZipEntryName("C:\\outside\\evil.txt")));
    assertFalse(UploadConfigSet.isSafeZipEntryPath("conf/../evil.txt"));
    assertTrue(UploadConfigSet.isSafeZipEntryPath("conf/good.txt"));
    assertTrue(UploadConfigSet.isSafeZipEntryPath("solrconfig.xml"));
  }

  @Test
  public void testSingleFileUploadRejectsTraversalPath() throws Exception {
    final String configSetName = "singlefile";
    createExistingConfigSet(configSetName, "solrconfig.xml", "<config/>");

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    byte[] data = "evil".getBytes(StandardCharsets.UTF_8);
    for (String filePath :
        new String[] {"../evil.txt", "conf/../../evil.txt", "..\\evil.txt", "C:/evil.txt"}) {
      final var ex =
          assertThrows(
              SolrException.class,
              () ->
                  api.uploadConfigSetFile(configSetName, filePath, new ByteArrayInputStream(data)));
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    }
    // Nothing was written outside of the configset, and the configset is unchanged.
    assertFalse(Files.exists(configSetBase.resolve("evil.txt")));
    assertTrue(Files.exists(configSetBase.resolve(configSetName).resolve("solrconfig.xml")));
  }

  @Test
  public void testSingleFileUploadNormalizesBackslashPath() throws Exception {
    final String configSetName = "singlefile";
    createExistingConfigSet(configSetName, "solrconfig.xml", "<config/>");

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    final var response =
        api.uploadConfigSetFile(
            configSetName,
            "conf\\stopwords.txt",
            new ByteArrayInputStream("a\nthe".getBytes(StandardCharsets.UTF_8)));

    assertNotNull(response);
    assertTrue(
        Files.exists(
            configSetBase.resolve(configSetName).resolve("conf").resolve("stopwords.txt")));
  }

  @Test
  public void testSingleFileUploadNestedPath() throws Exception {
    final String configSetName = "singlefile";
    createExistingConfigSet(configSetName, "solrconfig.xml", "<config/>");

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    final var response =
        api.uploadConfigSetFile(
            configSetName,
            "conf/good.txt",
            new ByteArrayInputStream("good".getBytes(StandardCharsets.UTF_8)));

    assertNotNull(response);
    assertTrue(
        Files.exists(configSetBase.resolve(configSetName).resolve("conf").resolve("good.txt")));
    assertEquals(
        "good",
        Files.readString(
            configSetBase.resolve(configSetName).resolve("conf").resolve("good.txt"),
            StandardCharsets.UTF_8));
  }
}

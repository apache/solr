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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
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
}

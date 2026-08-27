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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.FileSystemConfigSetService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link UploadConfigSet#uploadConfigSetFile} */
public class UploadConfigSetFileAPITest extends SolrTestCase {

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

  @Test
  public void testSingleFileUploadSuccess() throws Exception {
    final String configSetName = "singlefile";
    final String filePath = "solrconfig.xml";
    final String content = "<config/>";
    InputStream fileStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    api.uploadConfigSetFile(configSetName, filePath, fileStream);

    // Verify the file was uploaded
    byte[] uploadedData = configSetService.downloadFileFromConfig(configSetName, filePath);
    assertEquals(content, new String(uploadedData, StandardCharsets.UTF_8));
  }

  @Test
  public void testSingleFileWithEmptyPathThrowsBadRequest() {
    final String configSetName = "emptypath";
    InputStream fileStream = new ByteArrayInputStream("<config/>".getBytes(StandardCharsets.UTF_8));

    final var api = new UploadConfigSet(mockCoreContainer, null, null);

    // Test with empty filePath
    final var ex =
        assertThrows(
            SolrException.class, () -> api.uploadConfigSetFile(configSetName, "", fileStream));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue("Error should mention invalid path", ex.getMessage().contains("not valid"));
  }

  @Test
  public void testSingleFileUploadWithNestedPath() throws Exception {
    final String configSetName = "nested";
    final String filePath = "lang/stopwords_en.txt";
    final String content = "a\nthe\nis";
    InputStream fileStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    final var api = new UploadConfigSet(mockCoreContainer, null, null);
    api.uploadConfigSetFile(configSetName, filePath, fileStream);

    // Verify the file was uploaded with correct path
    byte[] uploadedData = configSetService.downloadFileFromConfig(configSetName, filePath);
    assertEquals(content, new String(uploadedData, StandardCharsets.UTF_8));
  }
}

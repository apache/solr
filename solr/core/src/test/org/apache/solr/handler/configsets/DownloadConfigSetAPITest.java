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

import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.FileSystemConfigSetService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link DownloadConfigSet}. */
public class DownloadConfigSetAPITest extends SolrTestCase {

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

  /** Creates a configset directory with a single file so the API can find and zip it. */
  private void createConfigSet(String name, String fileName, String content) throws Exception {
    Path dir = configSetBase.resolve(name);
    Files.createDirectories(dir);
    Files.writeString(dir.resolve(fileName), content, StandardCharsets.UTF_8);
  }

  @Test
  @SuppressWarnings("resource") // Response never created when exception is thrown
  public void testMissingConfigSetNameThrowsBadRequest() {
    final var api = new DownloadConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.downloadConfigSet(null));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());

    final var ex2 = assertThrows(SolrException.class, () -> api.downloadConfigSet(""));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex2.code());
  }

  @Test
  @SuppressWarnings("resource") // Response never created when exception is thrown
  public void testNonExistentConfigSetThrowsNotFound() {
    // "missing" was never created in configSetBase, so checkConfigExists returns false
    final var api = new DownloadConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.downloadConfigSet("missing"));
    assertEquals(SolrException.ErrorCode.NOT_FOUND.code, ex.code());
  }

  @Test
  public void testSuccessfulDownloadReturnsZipResponse() throws Exception {
    createConfigSet("myconfig", "solrconfig.xml", "<config/>");

    final var api = new DownloadConfigSet(mockCoreContainer, null, null);
    try (final Response response = api.downloadConfigSet("myconfig")) {
      assertEquals(200, response.getStatus());
      assertEquals("application/zip", response.getMediaType().toString());
    }
  }
}

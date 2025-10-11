/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.extraction;

import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.tika.config.TikaConfig;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for LocalTikaExtractionBackend independent of the HTTP handler. */
public class LocalTikaExtractionBackendTest extends SolrTestCaseJ4 {

  private static TikaConfig tikaConfig;
  private static ParseContextConfig parseContextConfig;

  @BeforeClass
  public static void setupClass() throws Exception {
    try (InputStream is =
        LocalTikaExtractionBackendTest.class
            .getClassLoader()
            .getResourceAsStream("solr-default-tika-config.xml")) {
      assertNotNull("solr-default-tika-config.xml not on classpath", is);
      tikaConfig = new TikaConfig(is);
    }
    parseContextConfig = new ParseContextConfig();
  }

  private LocalTikaExtractionBackend newBackend() {
    return new LocalTikaExtractionBackend(tikaConfig, parseContextConfig);
  }

  private ExtractionRequest newRequest(
      String resourceName,
      String streamType,
      String contentType,
      String charset,
      String streamName,
      String streamSourceInfo,
      Long streamSize,
      String resourcePassword,
      String returnType,
      boolean tikaserverRecursive,
      Map<String, String> tikaserverRequestHeaders) {
    return new ExtractionRequest(
        streamType,
        resourceName,
        contentType,
        charset,
        streamName,
        streamSourceInfo,
        streamSize,
        resourcePassword,
        null,
        returnType,
        tikaserverRecursive,
        null,
        tikaserverRequestHeaders);
  }

  @Test
  public void testWrongStreamTypeThrows() throws Exception {
    LocalTikaExtractionBackend backend = newBackend();
    try (InputStream in = Files.newInputStream(getFile("extraction/version_control.txt"))) {
      // Non-existing type -> no parser available
      ExtractionRequest req =
          newRequest(
              "version_control.txt",
              "foo/bar",
              null,
              null,
              "version_control.txt",
              null,
              null,
              null,
              "text",
              false,
              Collections.emptyMap());
      expectThrows(IllegalArgumentException.class, () -> backend.extract(in, req));
    }

    try (InputStream in = Files.newInputStream(getFile("extraction/version_control.txt"))) {
      // Wrong but existing type -> likely to fail when parsing
      ExtractionRequest req =
          newRequest(
              "version_control.txt",
              "application/pdf",
              null,
              null,
              "version_control.txt",
              null,
              null,
              null,
              "text",
              false,
              Collections.emptyMap());
      expectThrows(Exception.class, () -> backend.extract(in, req));
    }
  }

  @Test
  public void testPasswordProtectedDocxWithoutPasswordThrows() throws Exception {
    LocalTikaExtractionBackend backend = newBackend();
    try (InputStream in = Files.newInputStream(getFile("extraction/password-is-Word2010.docx"))) {
      ExtractionRequest req =
          newRequest(
              "password-is-Word2010.docx",
              null,
              null,
              null,
              "password-is-Word2010.docx",
              null,
              null,
              null,
              "text",
              false,
              Collections.emptyMap());
      expectThrows(Exception.class, () -> backend.extract(in, req));
    }
  }

  @Test
  public void testPasswordProtectedDocxWithPasswordSucceeds() throws Exception {
    LocalTikaExtractionBackend backend = newBackend();
    try (InputStream in = Files.newInputStream(getFile("extraction/password-is-Word2010.docx"))) {
      ExtractionRequest req =
          newRequest(
              "password-is-Word2010.docx",
              null,
              null,
              null,
              "password-is-Word2010.docx",
              null,
              null,
              "Word2010",
              "text",
              false,
              Collections.emptyMap());
      ExtractionResult res = backend.extract(in, req);
      assertNotNull(res);
      assertNotNull(res.getMetadata());
      String content = res.getContent();
      assertNotNull(content);
      assertTrue(
          "Content should mention password-protected doc text",
          content.contains("Test password protected word doc"));
    }
  }
}

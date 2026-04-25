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

package org.apache.solr.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCaseJ4;

public class FileTypeMagicUtilTest extends SolrTestCaseJ4 {
  public void testGuessMimeType() throws IOException {
    assertResourceMimeType("application/x-java-applet", "/magic/HelloWorldJavaClass.class.bin");
    assertResourceMimeType("application/zip", "/runtimecode/containerplugin.v.1.jar.bin");
    assertResourceMimeType("application/x-tar", "/magic/hello.tar.bin");
    assertResourceMimeType("text/x-shellscript", "/magic/shell.sh.txt");
  }

  public void testGuessMimeTypeBytes() {
    // Empty / null
    assertEquals("application/octet-stream", FileTypeMagicUtil.INSTANCE.guessMimeType(new byte[0]));
    assertFalse(FileTypeMagicUtil.isFileForbiddenInConfigset(new byte[0]));

    // Java class: 0xCAFEBABE + version 52 (Java 8)
    byte[] javaClass = {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE, 0, 0, 0, 52};
    assertEquals("application/x-java-applet", FileTypeMagicUtil.INSTANCE.guessMimeType(javaClass));

    // Java class: preview-compiled (minor=0xFFFF, major=61 / Java 17).
    // A previous version had a signed-integer overflow that allowed these through.
    byte[] previewClass = {
      (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE, (byte) 0xFF, (byte) 0xFF, 0, 61
    };
    assertEquals(
        "application/x-java-applet", FileTypeMagicUtil.INSTANCE.guessMimeType(previewClass));

    // ZIP: PK\x03\x04
    byte[] zip = {'P', 'K', 0x03, 0x04, 0, 0, 0, 0};
    assertEquals("application/zip", FileTypeMagicUtil.INSTANCE.guessMimeType(zip));

    // ZIP: PK\x05\x06 (empty archive)
    byte[] emptyZip = {'P', 'K', 0x05, 0x06, 0, 0, 0, 0};
    assertEquals("application/zip", FileTypeMagicUtil.INSTANCE.guessMimeType(emptyZip));

    // ZIP: PK\x07\x08 (data-descriptor signature)
    byte[] ddZip = {'P', 'K', 0x07, 0x08, 0, 0, 0, 0};
    assertEquals("application/zip", FileTypeMagicUtil.INSTANCE.guessMimeType(ddZip));

    // gzip-compressed archive
    byte[] gzip = {(byte) 0x1F, (byte) 0x8B, 0x08, 0x00};
    assertEquals("application/x-tar", FileTypeMagicUtil.INSTANCE.guessMimeType(gzip));

    // bzip2-compressed archive
    byte[] bzip2 = {'B', 'Z', 'h', '9'};
    assertEquals("application/x-tar", FileTypeMagicUtil.INSTANCE.guessMimeType(bzip2));

    // xz-compressed archive
    byte[] xz = {(byte) 0xFD, '7', 'z', 'X', 'Z', 0x00};
    assertEquals("application/x-tar", FileTypeMagicUtil.INSTANCE.guessMimeType(xz));

    // Shell scripts — various interpreter paths
    assertShellScript("#!/bin/sh\necho hello\n");
    assertShellScript("#!/usr/bin/env python3\nprint('hi')\n");
    assertShellScript("#!/opt/homebrew/bin/python3\nprint('hi')\n");
    assertShellScript("#! /bin/bash\necho hi\n");
    assertShellScript("#!/nix/store/xxx-bash/bin/bash\necho hi\n");

    // Plain text: not forbidden
    assertEquals(
        "application/octet-stream",
        FileTypeMagicUtil.INSTANCE.guessMimeType("hello world".getBytes(StandardCharsets.UTF_8)));
  }

  public void testIsFileForbiddenInConfigset() throws IOException {
    assertResourceForbiddenInConfigset("/magic/HelloWorldJavaClass.class.bin");
    assertResourceForbiddenInConfigset("/magic/shell.sh.txt");
    assertResourceAllowedInConfigset("/magic/plain.txt");
  }

  public void testPolyglotZipNotDetected() {
    // A JPEG+ZIP polyglot starts with JPEG magic — our offset-0 check does not detect the
    // appended ZIP. This is a known limitation: the file would be treated as its outer format
    // by Solr's configset processing and never extracted as a ZIP by Solr itself.
    byte[] jpegMagic = {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0};
    assertEquals("application/octet-stream", FileTypeMagicUtil.INSTANCE.guessMimeType(jpegMagic));
  }

  private void assertShellScript(String content) {
    assertEquals(
        "text/x-shellscript",
        FileTypeMagicUtil.INSTANCE.guessMimeType(content.getBytes(StandardCharsets.US_ASCII)));
  }

  private void assertResourceMimeType(String mimeType, String resourcePath) throws IOException {
    try (InputStream stream = FileTypeMagicUtil.class.getResourceAsStream(resourcePath)) {
      assertNotNull("Test resource not found: " + resourcePath, stream);
      assertEquals(mimeType, FileTypeMagicUtil.INSTANCE.guessMimeType(stream));
    }
  }

  private void assertResourceForbiddenInConfigset(String resourcePath) throws IOException {
    try (InputStream stream = FileTypeMagicUtil.class.getResourceAsStream(resourcePath)) {
      assertNotNull("Test resource not found: " + resourcePath, stream);
      assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(stream));
    }
  }

  private void assertResourceAllowedInConfigset(String resourcePath) throws IOException {
    try (InputStream stream = FileTypeMagicUtil.class.getResourceAsStream(resourcePath)) {
      assertNotNull("Test resource not found: " + resourcePath, stream);
      assertFalse(FileTypeMagicUtil.isFileForbiddenInConfigset(stream));
    }
  }
}

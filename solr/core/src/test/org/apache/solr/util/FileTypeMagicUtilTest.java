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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;

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
    assertFalse(FileTypeMagicUtil.isFileForbiddenInConfigset((byte[]) null));

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

    // gzip compressed file
    byte[] gzip = {(byte) 0x1F, (byte) 0x8B, 0x08, 0x00};
    assertEquals("application/gzip", FileTypeMagicUtil.INSTANCE.guessMimeType(gzip));
    assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(gzip));

    // bzip2 compressed file
    byte[] bzip2 = {'B', 'Z', 'h', '9'};
    assertEquals("application/x-bzip2", FileTypeMagicUtil.INSTANCE.guessMimeType(bzip2));
    assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(bzip2));

    // xz compressed file
    byte[] xz = {(byte) 0xFD, '7', 'z', 'X', 'Z', 0x00};
    assertEquals("application/x-xz", FileTypeMagicUtil.INSTANCE.guessMimeType(xz));
    assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(xz));

    // Shell scripts — various interpreter paths
    assertShellScript("#!/bin/sh\necho hello\n");
    assertShellScript("#!/usr/bin/env python3\nprint('hi')\n");
    assertShellScript("#!/opt/homebrew/bin/python3\nprint('hi')\n");
    assertShellScript("#! /bin/bash\necho hi\n");
    assertShellScript("#!/nix/store/xxx-bash/bin/bash\necho hi\n");

    // MZ: Windows EXE / self-extracting ZIP
    byte[] mz = {'M', 'Z', 0, 0};
    assertEquals("application/x-dosexec", FileTypeMagicUtil.INSTANCE.guessMimeType(mz));
    assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(mz));

    // ELF: Linux native binary
    byte[] elf = {0x7F, 'E', 'L', 'F', 0x02, 0x01};
    assertEquals("application/x-executable", FileTypeMagicUtil.INSTANCE.guessMimeType(elf));
    assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(elf));

    // Java serialized object
    byte[] ser = {(byte) 0xAC, (byte) 0xED, 0x00, 0x05};
    assertEquals(
        "application/x-java-serialized-object", FileTypeMagicUtil.INSTANCE.guessMimeType(ser));
    assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(ser));

    // Mach-O: all four variants (32/64-bit, big/little-endian)
    byte[] macho32be = {(byte) 0xFE, (byte) 0xED, (byte) 0xFA, (byte) 0xCE};
    byte[] macho64be = {(byte) 0xFE, (byte) 0xED, (byte) 0xFA, (byte) 0xCF};
    byte[] macho32le = {(byte) 0xCE, (byte) 0xFA, (byte) 0xED, (byte) 0xFE};
    byte[] macho64le = {(byte) 0xCF, (byte) 0xFA, (byte) 0xED, (byte) 0xFE};
    for (byte[] m : new byte[][] {macho32be, macho64be, macho32le, macho64le}) {
      assertEquals("application/x-mach-binary", FileTypeMagicUtil.INSTANCE.guessMimeType(m));
      assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(m));
    }

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

  public void testGuessMimeTypeTarBytes() {
    byte[] tar = new byte[512];
    tar[257] = 'u';
    tar[258] = 's';
    tar[259] = 't';
    tar[260] = 'a';
    tar[261] = 'r';
    assertEquals("application/x-tar", FileTypeMagicUtil.INSTANCE.guessMimeType(tar));
    assertTrue(FileTypeMagicUtil.isFileForbiddenInConfigset(tar));
  }

  public void testBuildForbiddenTypes() {
    // Valid subset — succeeds and returns exactly the configured types
    assertEquals(
        Set.of("application/zip", "application/gzip"),
        FileTypeMagicUtil.buildForbiddenTypes(List.of("application/zip", "application/gzip")));

    // Full default set — succeeds
    assertEquals(
        new HashSet<>(FileTypeMagicUtil.DEFAULT_FORBIDDEN_MIME_TYPES),
        FileTypeMagicUtil.buildForbiddenTypes(FileTypeMagicUtil.DEFAULT_FORBIDDEN_MIME_TYPES));

    // Unknown type — throws SolrException naming the bad type
    SolrException ex =
        assertThrows(
            SolrException.class,
            () -> FileTypeMagicUtil.buildForbiddenTypes(List.of("application/zip", "text/html")));
    assertTrue(ex.getMessage().contains("text/html"));
  }

  public void testAssertConfigSetFolderLegal() throws IOException {
    // Clean directory — must not throw
    Path dir = createTempDir();
    Files.writeString(dir.resolve("schema.xml"), "<schema/>");
    FileTypeMagicUtil.assertConfigSetFolderLegal(dir);

    // Forbidden file (Java class magic bytes) — SolrException
    byte[] javaClass = {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE, 0, 0, 0, 52};
    Files.write(dir.resolve("Evil.class"), javaClass);
    assertThrows(SolrException.class, () -> FileTypeMagicUtil.assertConfigSetFolderLegal(dir));

    // Symbolic link — SolrException (skip on platforms without symlink support)
    Path cleanDir = createTempDir();
    Files.writeString(cleanDir.resolve("safe.txt"), "safe");
    try {
      Files.createSymbolicLink(cleanDir.resolve("link.txt"), cleanDir.resolve("safe.txt"));
      assertThrows(
          SolrException.class, () -> FileTypeMagicUtil.assertConfigSetFolderLegal(cleanDir));
    } catch (UnsupportedOperationException | IOException | SecurityException ignored) {
      // symlinks not supported on this platform or blocked by security manager — skip
    }
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

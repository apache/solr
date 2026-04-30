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
package org.apache.solr.security.agent;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Tests that Windows UNC paths ({@code \\server\share\...} or {@code //server/share/...}) are
 * detected and rejected regardless of any policy rule.
 *
 * <p>UNC path blocking is a platform-independent invariant: even on Linux and macOS the detection
 * logic must correctly identify and block UNC-style path strings when they appear in an intercepted
 * operation (e.g. via a ByteBuddy-intercepted NIO call on Windows or a crafted path string).
 *
 * <p>Note: on POSIX systems Java's {@code Path} normalises {@code //} to {@code /}, so the
 * end-to-end enforcement test for forward-slash UNC paths is deferred to Windows CI. These tests
 * validate the core detection heuristic which underpins that enforcement.
 */
public class UncPathRejectionTest extends SolrTestCase {

  // ---------------------------------------------------------------------------
  // isUncPath detection (unit — platform-independent string check)
  // ---------------------------------------------------------------------------

  @Test
  public void testBackslashUncDetected() {
    assertTrue(FileAccessInterceptor.isUncPath("\\\\server\\share\\file"));
    assertTrue(FileAccessInterceptor.isUncPath("\\\\192.168.1.1\\c$\\data"));
    assertTrue(FileAccessInterceptor.isUncPath("\\\\?\\Volume{abc}\\file.txt"));
  }

  @Test
  public void testForwardSlashUncDetected() {
    assertTrue(FileAccessInterceptor.isUncPath("//server/share/file"));
    assertTrue(FileAccessInterceptor.isUncPath("//192.168.1.1/c$/data/secret"));
  }

  @Test
  public void testNormalUnixPathsNotDetectedAsUnc() {
    assertFalse(FileAccessInterceptor.isUncPath("/normal/path"));
    assertFalse(FileAccessInterceptor.isUncPath("/opt/solr/conf"));
    assertFalse(FileAccessInterceptor.isUncPath("/tmp/data"));
  }

  @Test
  public void testWindowsStyleDrivePathNotDetectedAsUnc() {
    // Drive-letter paths must NOT be falsely flagged
    assertFalse(FileAccessInterceptor.isUncPath("C:\\Windows\\System32"));
    assertFalse(FileAccessInterceptor.isUncPath("D:\\data\\backup"));
  }

  @Test
  public void testRelativePathNotDetectedAsUnc() {
    assertFalse(FileAccessInterceptor.isUncPath("relative/path/file.txt"));
    assertFalse(FileAccessInterceptor.isUncPath("data/backup"));
  }

  @Test
  public void testEmptyAndNullSafetyInIsUncPath() {
    assertFalse(FileAccessInterceptor.isUncPath(""));
    assertFalse(FileAccessInterceptor.isUncPath("/"));
    assertFalse(FileAccessInterceptor.isUncPath("\\"));
  }
}

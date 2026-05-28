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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

/**
 * Tests that symlink-escape attacks are blocked by {@link FileInterceptor}.
 *
 * <p>A symlink inside a permitted directory that points to a target outside permitted directories
 * must be denied even though the symlink path itself would have matched a permitted rule. The
 * interceptor resolves the real path via {@code Path.toRealPath()} before the policy check.
 *
 * <p>Tests that require symlink creation are skipped on filesystems that do not support symbolic
 * links (e.g. certain Windows configurations).
 */
public class SymlinkEscapeTest extends SolrTestCase {

  @After
  public void resetSingleton() {
    AgentPolicy.resetForTesting();
  }

  private void resetSingletonSilent() {
    AgentPolicy.resetForTesting();
  }

  private AgentPolicy buildPolicyPermitting(Path dir) {
    resetSingletonSilent();
    // Use the real (symlink-resolved) path so the policy matches after toRealPath() resolution
    String realDirStr;
    try {
      realDirStr = dir.toRealPath().toString();
    } catch (IOException e) {
      realDirStr = dir.toAbsolutePath().toString();
    }
    PermittedPath allowed = new PermittedPath(realDirStr, "read", true, PolicySource.DEFAULT);
    AgentPolicy policy =
        new AgentPolicy(
            List.of(allowed), List.of(), List.of(), List.of(), AgentPolicy.EnforcementMode.ENFORCE);
    AgentPolicy.initialize(policy);
    return policy;
  }

  // ---------------------------------------------------------------------------
  // Symlink pointing inside the permitted directory — should be allowed
  // ---------------------------------------------------------------------------

  @Test
  public void testSymlinkInsidePermittedDirIsAllowed() throws Exception {
    Path tmpDir = createTempDir();
    buildPolicyPermitting(tmpDir);

    Path realFile = tmpDir.resolve("real.txt");
    Files.writeString(realFile, "data");
    Path symlink = tmpDir.resolve("link.txt");

    try {
      Files.createSymbolicLink(symlink, realFile);
    } catch (UnsupportedOperationException | SecurityException e) {
      Assume.assumeTrue("Symlinks not supported or permitted in this test environment", false);
      return;
    }

    // Symlink resolves to a path inside the permitted dir — should NOT throw
    InterceptorTestHelper.checkPath(
        symlink, "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }

  // ---------------------------------------------------------------------------
  // Symlink escaping the permitted directory — must be blocked
  // ---------------------------------------------------------------------------

  @Test(expected = SecurityException.class)
  public void testSymlinkEscapeBlockedInEnforceMode() throws Exception {
    Path permittedDir = createTempDir("permitted");
    Path otherDir = createTempDir("other");
    buildPolicyPermitting(permittedDir);

    // Create a real file outside the permitted directory
    Path outsideFile = otherDir.resolve("secret.txt");
    Files.writeString(outsideFile, "secret data");

    // Create a symlink INSIDE the permitted dir that points OUTSIDE it
    Path symlink = permittedDir.resolve("escaped.txt");
    try {
      Files.createSymbolicLink(symlink, outsideFile);
    } catch (UnsupportedOperationException | SecurityException e) {
      Assume.assumeTrue("Symlinks not supported or permitted in this test environment", false);
      // Satisfy the @Test(expected=SecurityException.class) — fake a throw
      throw new SecurityException("symlinks not available — skip");
    }

    // Accessing via the symlink path should be denied because the REAL path is outside permitted
    // dir
    InterceptorTestHelper.checkPath(
        symlink, "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }

  @Test
  public void testSymlinkEscapeIncrementsFileCounter() throws Exception {
    Path permittedDir = createTempDir("permitted");
    Path otherDir = createTempDir("other");
    buildPolicyPermitting(permittedDir);

    Path outsideFile = otherDir.resolve("secret.txt");
    Files.writeString(outsideFile, "secret");

    Path symlink = permittedDir.resolve("escaped.txt");
    try {
      Files.createSymbolicLink(symlink, outsideFile);
    } catch (UnsupportedOperationException | SecurityException e) {
      Assume.assumeTrue("Symlinks not supported or permitted in this test environment", false);
      return;
    }

    long before = ViolationMetricsReporter.fileCount();
    try {
      InterceptorTestHelper.checkPath(
          symlink, "read", SecurityViolationLogger.ViolationType.FILE_READ);
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.fileCount());
  }

  // ---------------------------------------------------------------------------
  // Non-existent path — graceful fallback to normalized path check
  // ---------------------------------------------------------------------------

  @Test
  public void testNonExistentPathFallsBackToNormalizedCheck() {
    Path tmpDir = createTempDir();
    buildPolicyPermitting(tmpDir);

    // Non-existent file inside the permitted dir — toRealPath() fails → normalized path used
    Path nonExistent = tmpDir.resolve("does-not-exist.txt");
    // Should NOT throw — the normalized path falls within the permitted dir
    InterceptorTestHelper.checkPath(
        nonExistent, "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }
}

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

import java.nio.file.Path;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Test;

/**
 * Integration-level tests for the Solr security agent in enforce mode.
 *
 * <p>These tests exercise the policy engine and interceptor check logic end-to-end without
 * requiring the ByteBuddy instrumentation to be active (which requires premain). They validate that
 * permitted operations pass, denied operations throw {@link SecurityException}, and that the {@link
 * ViolationMetricsReporter} counters increment correctly on each violation.
 */
public class SolrAgentIntegrationTest extends SolrTestCase {

  @After
  public void resetSingleton() {
    SolrSecurityPolicy.resetForTesting();
  }

  private SolrSecurityPolicy buildEnforcePolicy(
      List<PermittedPath> paths,
      List<PermittedEndpoint> endpoints,
      List<ApprovedCallSite> exitCallers,
      List<ApprovedCallSite> execCallers) {
    SolrSecurityPolicy p =
        new SolrSecurityPolicy(
            paths, endpoints, exitCallers, execCallers, SolrSecurityPolicy.EnforcementMode.ENFORCE);
    SolrSecurityPolicy.initialize(p);
    return p;
  }

  // ---------------------------------------------------------------------------
  // File access tests
  // ---------------------------------------------------------------------------

  @Test
  public void testPermittedFileReadSucceeds() {
    Path tmpDir = createTempDir();
    PermittedPath allowed =
        new PermittedPath(tmpDir.toString(), "read", true, PolicyLoader.PolicySource.DEFAULT);
    buildEnforcePolicy(List.of(allowed), List.of(), List.of(), List.of());

    Path target = tmpDir.resolve("test.txt");
    // checkPath should not throw for a path inside the permitted dir
    FileAccessInterceptor.checkPath(
        target, "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }

  @Test(expected = SecurityException.class)
  public void testDeniedFileReadThrows() {
    Path tmpDir = createTempDir();
    // Policy permits nothing
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    FileAccessInterceptor.checkPath(
        tmpDir.resolve("secret.txt"), "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }

  @Test
  public void testDeniedFileReadIncrementsFileCounter() {
    long before = ViolationMetricsReporter.fileCount();
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    try {
      FileAccessInterceptor.checkPath(
          Path.of("/etc/passwd"), "read", SecurityViolationLogger.ViolationType.FILE_READ);
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.fileCount());
  }

  // ---------------------------------------------------------------------------
  // UNC path tests
  // ---------------------------------------------------------------------------

  @Test
  public void testUncPathAlwaysBlocked() {
    PermittedPath all = new PermittedPath("/", "read", true, PolicyLoader.PolicySource.DEFAULT);
    buildEnforcePolicy(List.of(all), List.of(), List.of(), List.of());
    // Even with a broad policy, UNC paths are blocked unconditionally
    assertTrue(FileAccessInterceptor.isUncPath("\\\\server\\share\\file"));
    assertTrue(FileAccessInterceptor.isUncPath("//server/share/file"));
    assertFalse(FileAccessInterceptor.isUncPath("/normal/path"));
  }

  // ---------------------------------------------------------------------------
  // Network tests
  // ---------------------------------------------------------------------------

  @Test
  public void testPermittedEndpointNotBlocked() {
    PermittedEndpoint ep =
        new PermittedEndpoint("*:8983", "connect,resolve", null, PolicyLoader.PolicySource.DEFAULT);
    buildEnforcePolicy(List.of(), List.of(ep), List.of(), List.of());
    assertTrue(
        NetworkAccessInterceptor.isEndpointPermitted(
            SolrSecurityPolicy.getInstance(), "10.0.0.1", 8983));
  }

  @Test(expected = SecurityException.class)
  public void testDeniedNetworkConnectThrowsInEnforceMode() throws Exception {
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    java.net.InetSocketAddress addr =
        new java.net.InetSocketAddress(java.net.InetAddress.getByName("10.0.0.99"), 9999);
    NetworkAccessInterceptor.checkConnect(addr);
  }

  @Test
  public void testDeniedNetworkIncrementsCounter() throws Exception {
    long before = ViolationMetricsReporter.networkCount();
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    java.net.InetSocketAddress addr =
        new java.net.InetSocketAddress(java.net.InetAddress.getByName("10.0.0.99"), 9999);
    try {
      NetworkAccessInterceptor.checkConnect(addr);
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.networkCount());
  }

  // ---------------------------------------------------------------------------
  // System.exit tests
  // ---------------------------------------------------------------------------

  @Test(expected = SecurityException.class)
  public void testUnapprovedExitThrowsInEnforceMode() {
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    ExitInterceptor.checkExit("System.exit(0)");
  }

  @Test
  public void testUnapprovedExitIncrementsCounter() {
    long before = ViolationMetricsReporter.exitCount();
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    try {
      ExitInterceptor.checkExit("System.exit(0)");
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.exitCount());
  }

  // ---------------------------------------------------------------------------
  // ProcessBuilder tests
  // ---------------------------------------------------------------------------

  @Test(expected = SecurityException.class)
  public void testUnapprovedProcessExecThrowsInEnforceMode() {
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    ProcessExecInterceptor.checkExec("ProcessBuilder.start()");
  }

  @Test
  public void testUnapprovedProcessExecIncrementsCounter() {
    long before = ViolationMetricsReporter.execCount();
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    try {
      ProcessExecInterceptor.checkExec("ProcessBuilder.start()");
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.execCount());
  }
}

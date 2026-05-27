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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
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
    AgentPolicy.resetForTesting();
  }

  private AgentPolicy buildEnforcePolicy(
      List<PermittedPath> paths,
      List<PermittedEndpoint> endpoints,
      List<ApprovedCallSite> exitCallers,
      List<ApprovedCallSite> execCallers) {
    AgentPolicy p =
        new AgentPolicy(
            paths, endpoints, exitCallers, execCallers, AgentPolicy.EnforcementMode.ENFORCE);
    AgentPolicy.initialize(p);
    return p;
  }

  // ---------------------------------------------------------------------------
  // File access tests
  // ---------------------------------------------------------------------------

  @Test
  public void testPermittedFileReadSucceeds() {
    Path tmpDir = createTempDir();
    PermittedPath allowed =
        new PermittedPath(tmpDir.toString(), "read", true, PolicySource.DEFAULT);
    buildEnforcePolicy(List.of(allowed), List.of(), List.of(), List.of());

    Path target = tmpDir.resolve("test.txt");
    // checkPath should not throw for a path inside the permitted dir
    FileInterceptor.checkPath(target, "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }

  @Test(expected = SecurityException.class)
  public void testDeniedFileReadThrows() {
    Path tmpDir = createTempDir();
    // Policy permits nothing
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    FileInterceptor.checkPath(
        tmpDir.resolve("secret.txt"), "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }

  @Test
  public void testDeniedFileReadIncrementsFileCounter() {
    long before = ViolationMetricsReporter.fileCount();
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    try {
      FileInterceptor.checkPath(
          Path.of("/etc/passwd"), "read", SecurityViolationLogger.ViolationType.FILE_READ);
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.fileCount());
  }

  // ---------------------------------------------------------------------------
  // Network tests
  // ---------------------------------------------------------------------------

  @Test
  public void testPermittedEndpointNotBlocked() {
    PermittedEndpoint ep =
        new PermittedEndpoint("*:8983", "connect,resolve", null, PolicySource.DEFAULT);
    buildEnforcePolicy(List.of(), List.of(ep), List.of(), List.of());
    assertTrue(
        SocketChannelInterceptor.isEndpointPermitted(AgentPolicy.getInstance(), "10.0.0.1", 8983));
  }

  @Test(expected = SecurityException.class)
  public void testDeniedNetworkConnectThrowsInEnforceMode() throws Exception {
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName("10.0.0.99"), 9999);
    SocketChannelInterceptor.checkConnect(addr);
  }

  @Test
  public void testDeniedNetworkIncrementsCounter() throws Exception {
    long before = ViolationMetricsReporter.networkCount();
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName("10.0.0.99"), 9999);
    try {
      SocketChannelInterceptor.checkConnect(addr);
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.networkCount());
  }

  // ---------------------------------------------------------------------------
  // System.exit tests — tested via AgentPolicy.isChainThatCanExit()
  // ---------------------------------------------------------------------------

  @Test
  public void testUnapprovedExitChainDenied() {
    buildEnforcePolicy(List.of(), List.of(), List.of(), List.of());
    assertFalse(AgentPolicy.getInstance().isChainThatCanExit(Set.of(String.class)));
  }

  @Test
  public void testApprovedExitChainPermitted() {
    List<ApprovedCallSite> exitCallers =
        List.of(
            new ApprovedCallSite(
                SolrAgentIntegrationTest.class.getName(),
                ApprovedCallSite.Operation.EXIT,
                PolicySource.DEFAULT));
    buildEnforcePolicy(List.of(), List.of(), exitCallers, List.of());
    assertTrue(
        AgentPolicy.getInstance().isChainThatCanExit(Set.of(SolrAgentIntegrationTest.class)));
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

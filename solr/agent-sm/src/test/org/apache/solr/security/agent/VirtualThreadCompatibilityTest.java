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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Test;

/**
 * Verifies that the Solr security agent enforcement logic is compatible with Java virtual threads
 * (Project Loom, available since Java 21).
 *
 * <p>{@link StackWalker} is virtual-thread–safe by specification (JEP 425). These tests exercise
 * the file and network interceptors from virtual threads to confirm:
 *
 * <ul>
 *   <li>Permitted operations succeed from a virtual thread context.
 *   <li>Denied operations throw {@link SecurityException} and increment counters correctly.
 *   <li>No {@link NullPointerException} or {@link ClassCastException} arises from stack walking on
 *       virtual-thread frames.
 * </ul>
 */
public class VirtualThreadCompatibilityTest extends SolrTestCase {

  @After
  public void resetSingleton() {
    AgentPolicy.resetForTesting();
  }

  private void resetSingletonSilent() {
    AgentPolicy.resetForTesting();
  }

  /** Runs {@code task} on a virtual thread and re-throws any exception it produces. */
  private static void runOnVirtualThread(RunnableWithException task) throws Exception {
    AtomicReference<Throwable> caught = new AtomicReference<>();
    Thread vt =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    task.run();
                  } catch (Throwable t) {
                    caught.set(t);
                  }
                });
    vt.join();
    Throwable t = caught.get();
    if (t instanceof Exception e) throw e;
    if (t instanceof Error e) throw e;
    if (t != null) throw new RuntimeException(t);
  }

  @FunctionalInterface
  interface RunnableWithException {
    void run() throws Exception;
  }

  // ---------------------------------------------------------------------------
  // File access from virtual threads
  // ---------------------------------------------------------------------------

  @Test
  public void testPermittedFileAccessFromVirtualThread() throws Exception {
    Path tmpDir = createTempDir();
    resetSingletonSilent();
    // Use the real (symlink-resolved) path so the policy matches after toRealPath() resolution
    String realDirStr;
    try {
      realDirStr = tmpDir.toRealPath().toString();
    } catch (java.io.IOException e) {
      realDirStr = tmpDir.toAbsolutePath().toString();
    }
    PermittedPath allowed =
        new PermittedPath(realDirStr, "read", true, PolicyLoader.PolicySource.DEFAULT);
    AgentPolicy policy =
        new AgentPolicy(
            List.of(allowed), List.of(), List.of(), List.of(), AgentPolicy.EnforcementMode.ENFORCE);
    AgentPolicy.initialize(policy);

    // Create the file so toRealPath() resolves correctly (no fallback to unresolved path)
    Path testFile = tmpDir.resolve("test.txt");
    java.nio.file.Files.writeString(testFile, "data");

    // Must not throw — permitted path from a virtual thread
    runOnVirtualThread(
        () ->
            FileInterceptor.checkPath(
                testFile, "read", SecurityViolationLogger.ViolationType.FILE_READ));
  }

  @Test
  public void testDeniedFileAccessFromVirtualThreadIncrementsCounter() throws Exception {
    resetSingletonSilent();
    AgentPolicy policy =
        new AgentPolicy(
            List.of(), List.of(), List.of(), List.of(), AgentPolicy.EnforcementMode.ENFORCE);
    AgentPolicy.initialize(policy);

    long before = ViolationMetricsReporter.fileCount();
    try {
      runOnVirtualThread(
          () ->
              FileInterceptor.checkPath(
                  Path.of("/tmp/denied-vt.txt"),
                  "read",
                  SecurityViolationLogger.ViolationType.FILE_READ));
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.fileCount());
  }

  @Test
  public void testDeniedFileAccessFromVirtualThreadNoStackWalkerException() throws Exception {
    resetSingletonSilent();
    AgentPolicy policy =
        new AgentPolicy(
            List.of(), List.of(), List.of(), List.of(), AgentPolicy.EnforcementMode.WARN);
    AgentPolicy.initialize(policy);

    // In warn mode, no SecurityException is thrown, but we must verify no NPE/CCE from StackWalker
    AtomicReference<Throwable> unexpected = new AtomicReference<>();
    Thread vt =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    FileInterceptor.checkPath(
                        Path.of("/tmp/vt-check.txt"),
                        "read",
                        SecurityViolationLogger.ViolationType.FILE_READ);
                  } catch (SecurityException ignored) {
                    // Expected in enforce mode
                  } catch (Throwable t) {
                    unexpected.set(t);
                  }
                });
    vt.join();
    assertNull(
        "Unexpected exception from virtual-thread stack walk: " + unexpected.get(),
        unexpected.get());
  }

  // ---------------------------------------------------------------------------
  // Network access from virtual threads
  // ---------------------------------------------------------------------------

  @Test
  public void testPermittedNetworkFromVirtualThread() throws Exception {
    resetSingletonSilent();
    PermittedEndpoint ep =
        new PermittedEndpoint("*:8983", "connect,resolve", null, PolicyLoader.PolicySource.DEFAULT);
    AgentPolicy policy =
        new AgentPolicy(
            List.of(), List.of(ep), List.of(), List.of(), AgentPolicy.EnforcementMode.ENFORCE);
    AgentPolicy.initialize(policy);

    runOnVirtualThread(
        () ->
            assertTrue(
                SocketChannelInterceptor.isEndpointPermitted(
                    AgentPolicy.getInstance(), "10.0.0.1", 8983)));
  }

  @Test
  public void testDeniedNetworkFromVirtualThreadIncrementsCounter() throws Exception {
    resetSingletonSilent();
    AgentPolicy policy =
        new AgentPolicy(
            List.of(), List.of(), List.of(), List.of(), AgentPolicy.EnforcementMode.ENFORCE);
    AgentPolicy.initialize(policy);

    long before = ViolationMetricsReporter.networkCount();
    try {
      runOnVirtualThread(
          () -> {
            InetSocketAddress addr =
                new InetSocketAddress(InetAddress.getByName("10.0.0.99"), 9999);
            SocketChannelInterceptor.checkConnect(addr);
          });
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.networkCount());
  }
}

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

import java.util.List;
import org.apache.solr.SolrTestCase;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link ExitInterceptor} enforcement logic. */
public class ExitInterceptorTest extends SolrTestCase {

  private long exitCountBefore;

  @Before
  public void snapshotCounters() {
    exitCountBefore = ViolationMetricsReporter.exitCount();
  }

  private void initPolicy(
      boolean approved, String callerClass, SolrSecurityPolicy.EnforcementMode mode) {
    // Reset singleton if already set
    resetPolicySingleton();
    List<ApprovedCallSite> exitCallers =
        approved
            ? List.of(
                new ApprovedCallSite(
                    callerClass,
                    ApprovedCallSite.Operation.EXIT,
                    PolicyLoader.PolicySource.DEFAULT))
            : List.of();
    SolrSecurityPolicy policy =
        new SolrSecurityPolicy(List.of(), List.of(), exitCallers, List.of(), mode);
    SolrSecurityPolicy.initialize(policy);
  }

  /** Resets the static singleton so each test starts fresh. */
  private static void resetPolicySingleton() {
    SolrSecurityPolicy.resetForTesting();
  }

  @Test
  public void testApprovedCallerDoesNotIncreaseCounter() {
    // Use the test class itself as an approved caller
    initPolicy(
        true, ExitInterceptorTest.class.getName(), SolrSecurityPolicy.EnforcementMode.ENFORCE);
    // Directly exercise checkExit with the approved class on the call stack
    // We call checkExit with a simulated target — no counter should increment
    ExitInterceptor.checkExit("System.exit(0)");
    assertEquals(exitCountBefore, ViolationMetricsReporter.exitCount());
    resetPolicySingleton();
  }

  @Test
  public void testUnapprovedCallerInWarnModeIncrementsCounter() {
    initPolicy(false, "some.other.Class", SolrSecurityPolicy.EnforcementMode.WARN);
    ExitInterceptor.checkExit("System.exit(0)");
    assertEquals(exitCountBefore + 1, ViolationMetricsReporter.exitCount());
    resetPolicySingleton();
  }

  @Test(expected = SecurityException.class)
  public void testUnapprovedCallerInEnforceModeThrows() {
    initPolicy(false, "some.other.Class", SolrSecurityPolicy.EnforcementMode.ENFORCE);
    try {
      ExitInterceptor.checkExit("System.exit(0)");
    } finally {
      resetPolicySingleton();
    }
  }

  @Test
  public void testUnapprovedCallerInEnforceModeIncrementsCounter() {
    initPolicy(false, "some.other.Class", SolrSecurityPolicy.EnforcementMode.ENFORCE);
    long before = ViolationMetricsReporter.exitCount();
    try {
      ExitInterceptor.checkExit("System.exit(0)");
    } catch (SecurityException ignored) {
      // expected
    }
    assertEquals(before + 1, ViolationMetricsReporter.exitCount());
    resetPolicySingleton();
  }

  @Test
  public void testRuntimeHaltCallsCheckExit() {
    initPolicy(false, "nobody", SolrSecurityPolicy.EnforcementMode.WARN);
    long before = ViolationMetricsReporter.exitCount();
    ExitInterceptor.checkExit("Runtime.halt(0)"); // same code path
    assertEquals(before + 1, ViolationMetricsReporter.exitCount());
    resetPolicySingleton();
  }
}

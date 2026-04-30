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

/** Unit tests for {@link ProcessExecInterceptor} enforcement logic. */
public class ProcessExecInterceptorTest extends SolrTestCase {

  private long execCountBefore;

  @Before
  public void snapshotCounters() {
    execCountBefore = ViolationMetricsReporter.execCount();
  }

  private void initPolicy(
      boolean approved, String callerClass, SolrSecurityPolicy.EnforcementMode mode) {
    resetPolicySingleton();
    List<ApprovedCallSite> execCallers =
        approved
            ? List.of(
                new ApprovedCallSite(
                    callerClass,
                    ApprovedCallSite.Operation.EXEC,
                    PolicyLoader.PolicySource.DEFAULT))
            : List.of();
    SolrSecurityPolicy policy =
        new SolrSecurityPolicy(List.of(), List.of(), List.of(), execCallers, mode);
    SolrSecurityPolicy.initialize(policy);
  }

  private static void resetPolicySingleton() {
    SolrSecurityPolicy.resetForTesting();
  }

  @Test
  public void testApprovedCallerDoesNotIncreaseCounter() {
    initPolicy(
        true,
        ProcessExecInterceptorTest.class.getName(),
        SolrSecurityPolicy.EnforcementMode.ENFORCE);
    ProcessExecInterceptor.checkExec("ProcessBuilder.start()");
    assertEquals(execCountBefore, ViolationMetricsReporter.execCount());
    resetPolicySingleton();
  }

  @Test
  public void testUnapprovedCallerInWarnModeIncrementsCounter() {
    initPolicy(false, "nobody", SolrSecurityPolicy.EnforcementMode.WARN);
    ProcessExecInterceptor.checkExec("ProcessBuilder.start()");
    assertEquals(execCountBefore + 1, ViolationMetricsReporter.execCount());
    resetPolicySingleton();
  }

  @Test(expected = SecurityException.class)
  public void testUnapprovedCallerInEnforceModeThrows() {
    initPolicy(false, "nobody", SolrSecurityPolicy.EnforcementMode.ENFORCE);
    try {
      ProcessExecInterceptor.checkExec("ProcessBuilder.start()");
    } finally {
      resetPolicySingleton();
    }
  }

  @Test
  public void testRuntimeExecBlocked() {
    initPolicy(false, "nobody", SolrSecurityPolicy.EnforcementMode.WARN);
    long before = ViolationMetricsReporter.execCount();
    ProcessExecInterceptor.checkExec("Runtime.exec(ls)");
    assertEquals(before + 1, ViolationMetricsReporter.execCount());
    resetPolicySingleton();
  }

  @Test
  public void testWildcardApprovalMatchesAny() {
    initPolicy(true, "*", SolrSecurityPolicy.EnforcementMode.ENFORCE);
    // Should not throw even for an unknown caller
    ProcessExecInterceptor.checkExec("ProcessBuilder.start()");
    assertEquals(execCountBefore, ViolationMetricsReporter.execCount());
    resetPolicySingleton();
  }
}

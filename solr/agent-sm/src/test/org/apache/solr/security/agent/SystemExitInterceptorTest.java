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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link SystemExitInterceptor} and {@link RuntimeHaltInterceptor} policy logic. */
public class SystemExitInterceptorTest extends SolrTestCase {

  @Before
  public void snapshotCounters() {}

  @After
  public void resetSingleton() {
    AgentPolicy.resetForTesting();
  }

  private AgentPolicy initPolicy(
      boolean approved, String callerClass, AgentPolicy.EnforcementMode mode) {
    AgentPolicy.resetForTesting();
    List<ApprovedCallSite> exitCallers =
        approved
            ? List.of(
                new ApprovedCallSite(
                    callerClass,
                    ApprovedCallSite.Operation.EXIT,
                    PolicyLoader.PolicySource.DEFAULT))
            : List.of();
    AgentPolicy policy = new AgentPolicy(List.of(), List.of(), exitCallers, List.of(), mode);
    AgentPolicy.initialize(policy);
    return policy;
  }

  @Test
  public void testApprovedCallerInChainPermits() {
    AgentPolicy policy =
        initPolicy(
            true, SystemExitInterceptorTest.class.getName(), AgentPolicy.EnforcementMode.ENFORCE);
    Collection<Class<?>> chain = Set.of(SystemExitInterceptorTest.class);
    assertTrue(policy.isChainThatCanExit(chain));
  }

  @Test
  public void testUnapprovedCallerNotInChainDenies() {
    AgentPolicy policy = initPolicy(false, "some.other.Class", AgentPolicy.EnforcementMode.ENFORCE);
    Collection<Class<?>> chain = Set.of(SystemExitInterceptorTest.class);
    assertFalse(policy.isChainThatCanExit(chain));
  }

  @Test
  public void testApprovedCallerAnywhereInChainPermits() {
    AgentPolicy policy =
        initPolicy(
            true, SystemExitInterceptorTest.class.getName(), AgentPolicy.EnforcementMode.ENFORCE);
    // Approved class is deep in the chain — should still grant permission
    Collection<Class<?>> chain =
        Set.of(String.class, Integer.class, SystemExitInterceptorTest.class);
    assertTrue(policy.isChainThatCanExit(chain));
  }

  @Test
  public void testWildcardPatternMatchesPackage() {
    AgentPolicy.resetForTesting();
    List<ApprovedCallSite> exitCallers =
        List.of(
            new ApprovedCallSite(
                "org.apache.solr.security.agent.*",
                ApprovedCallSite.Operation.EXIT,
                PolicyLoader.PolicySource.DEFAULT));
    AgentPolicy policy =
        new AgentPolicy(
            List.of(), List.of(), exitCallers, List.of(), AgentPolicy.EnforcementMode.ENFORCE);
    AgentPolicy.initialize(policy);

    Collection<Class<?>> chain = Set.of(SystemExitInterceptorTest.class);
    assertTrue(policy.isChainThatCanExit(chain));
  }

  @Test
  public void testEmptyChainDenies() {
    AgentPolicy policy =
        initPolicy(
            true, SystemExitInterceptorTest.class.getName(), AgentPolicy.EnforcementMode.ENFORCE);
    assertFalse(policy.isChainThatCanExit(Set.of()));
  }
}

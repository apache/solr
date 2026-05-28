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
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Test;

/** Unit tests for {@link SocketChannelInterceptor} policy matching logic. */
public class SocketChannelInterceptorTest extends SolrTestCase {

  @After
  public void resetSingleton() {
    AgentPolicy.resetForTesting();
  }

  private AgentPolicy policyWithEndpoint(String hostPort) {
    PermittedEndpoint ep =
        new PermittedEndpoint(hostPort, "connect,resolve", null, PolicySource.DEFAULT);
    return new AgentPolicy(
        List.of(), List.of(ep), List.of(), List.of(), AgentPolicy.EnforcementMode.ENFORCE);
  }

  @Test
  public void testLoopbackPermittedViaTrustedHosts() throws Exception {
    AgentPolicy policy =
        new AgentPolicy(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            AgentPolicy.EnforcementMode.ENFORCE,
            Set.of(),
            Set.of("127.0.0.1", "localhost", "::1"));
    AgentPolicy.initialize(policy);

    InetSocketAddress loopback = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8983);
    assertTrue(policy.trustedHosts().contains(loopback.getHostString()));
    // checkConnect skips trusted hosts before policy lookup
    InterceptorTestHelper.checkConnect(loopback); // must not throw
  }

  @Test
  public void testExactHostPortPermitted() {
    AgentPolicy policy = policyWithEndpoint("192.168.1.100:8983");
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "192.168.1.100", 8983));
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "192.168.1.100", 8984));
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "192.168.1.101", 8983));
  }

  @Test
  public void testWildcardHostPortPermitted() {
    AgentPolicy policy = policyWithEndpoint("*:8983");
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "10.0.0.5", 8983));
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "some-other-host", 8983));
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "10.0.0.5", 9983));
  }

  @Test
  public void testPortRangePermitted() {
    AgentPolicy policy = policyWithEndpoint("192.168.1.1:8000-9000");
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "192.168.1.1", 8983));
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "192.168.1.1", 8000));
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "192.168.1.1", 9000));
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "192.168.1.1", 7999));
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "192.168.1.1", 9001));
  }

  @Test
  public void testCodebaseScopedEntryDeniedWhenCallerNotFromThatCodeBase() {
    // A codeBase grant for a path where no class in the current test stack is loaded from.
    // The call chain contains test-framework classes, not classes from /nonexistent/path.
    PermittedEndpoint codeBasedEp =
        new PermittedEndpoint(
            "*",
            "connect,resolve",
            "file:/nonexistent/solr/modules/jwt-auth/-",
            PolicySource.DEFAULT);
    AgentPolicy policy =
        new AgentPolicy(
            List.of(),
            List.of(codeBasedEp),
            List.of(),
            List.of(),
            AgentPolicy.EnforcementMode.ENFORCE);
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "external.host", 443));
  }

  @Test
  public void testIsCallerFromCodeBaseMatchesTestClassCodeSource() {
    // Get the actual code source URL of this test class and verify isCallerFromCodeBase accepts it.
    URL loc =
        SocketChannelInterceptorTest.class.getProtectionDomain().getCodeSource().getLocation();
    String codeBase = "file:" + loc.getPath();
    // Strip any trailing "/" so it acts as an exact-directory match
    if (codeBase.endsWith("/")) codeBase = codeBase.substring(0, codeBase.length() - 1);
    // Append "/-" to make it a recursive match (any class under the test build dir)
    codeBase = codeBase + "/-";

    Collection<Class<?>> chain = Set.of(SocketChannelInterceptorTest.class);
    assertTrue(SocketChannelInterceptor.isCallerFromCodeBase(chain, codeBase));
  }

  @Test
  public void testIsCallerFromCodeBaseNoMatchForUnrelatedPath() {
    Collection<Class<?>> chain = Set.of(SocketChannelInterceptorTest.class);
    assertFalse(SocketChannelInterceptor.isCallerFromCodeBase(chain, "file:/nonexistent/path/-"));
  }

  @Test
  public void testUnlistedHostPortBlocked() {
    AgentPolicy policy = policyWithEndpoint("localhost:8983");
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "10.0.0.1", 443));
  }

  @Test
  public void testBroadWildcardPermitsAll() {
    AgentPolicy policy = policyWithEndpoint("*");
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "anything.com", 443));
  }

  @Test
  public void testIPv6BracketNotationMatches() {
    // Policy entries for IPv6 use bracket notation "[::1]:port"; the actual host from
    // InetSocketAddress.getHostString() is without brackets. Verify stripping works.
    AgentPolicy policy = policyWithEndpoint("[::1]:8983");
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "::1", 8983));
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "::1", 8984));
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "::2", 8983));
  }

  @Test
  public void testIPv6BracketWithPortRange() {
    AgentPolicy policy = policyWithEndpoint("[::1]:1-65535");
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "::1", 8983));
    assertTrue(SocketChannelInterceptor.isEndpointPermitted(policy, "::1", 1));
    assertFalse(SocketChannelInterceptor.isEndpointPermitted(policy, "::2", 8983));
  }
}

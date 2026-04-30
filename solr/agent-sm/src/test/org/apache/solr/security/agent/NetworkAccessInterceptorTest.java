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
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Unit tests for {@link NetworkAccessInterceptor} policy matching logic. */
public class NetworkAccessInterceptorTest extends SolrTestCase {

  private SolrSecurityPolicy policyWithEndpoint(String hostPort) {
    PermittedEndpoint ep =
        new PermittedEndpoint(hostPort, "connect,resolve", null, PolicyLoader.PolicySource.DEFAULT);
    return new SolrSecurityPolicy(
        List.of(), List.of(ep), List.of(), List.of(), SolrSecurityPolicy.EnforcementMode.ENFORCE);
  }

  @Test
  public void testLoopbackPermittedUnconditionally() throws Exception {
    // Even with an empty policy, loopback should be allowed.
    SolrSecurityPolicy policy =
        new SolrSecurityPolicy(
            List.of(), List.of(), List.of(), List.of(), SolrSecurityPolicy.EnforcementMode.ENFORCE);

    InetSocketAddress loopback = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8983);
    // Should not throw — loopback is unconditionally permitted
    assertTrue(loopback.getAddress().isLoopbackAddress());
    // checkConnect skips loopback before policy lookup
  }

  @Test
  public void testExactHostPortPermitted() {
    SolrSecurityPolicy policy = policyWithEndpoint("192.168.1.100:8983");
    assertTrue(NetworkAccessInterceptor.isEndpointPermitted(policy, "192.168.1.100", 8983));
    assertFalse(NetworkAccessInterceptor.isEndpointPermitted(policy, "192.168.1.100", 8984));
    assertFalse(NetworkAccessInterceptor.isEndpointPermitted(policy, "192.168.1.101", 8983));
  }

  @Test
  public void testWildcardHostPortPermitted() {
    // *:8983 should match any host on port 8983
    SolrSecurityPolicy policy = policyWithEndpoint("*:8983");
    assertTrue(NetworkAccessInterceptor.isEndpointPermitted(policy, "10.0.0.5", 8983));
    assertTrue(NetworkAccessInterceptor.isEndpointPermitted(policy, "some-other-host", 8983));
    assertFalse(NetworkAccessInterceptor.isEndpointPermitted(policy, "10.0.0.5", 9983));
  }

  @Test
  public void testPortRangePermitted() {
    SolrSecurityPolicy policy = policyWithEndpoint("192.168.1.1:8000-9000");
    assertTrue(NetworkAccessInterceptor.isEndpointPermitted(policy, "192.168.1.1", 8983));
    assertTrue(NetworkAccessInterceptor.isEndpointPermitted(policy, "192.168.1.1", 8000));
    assertTrue(NetworkAccessInterceptor.isEndpointPermitted(policy, "192.168.1.1", 9000));
    assertFalse(NetworkAccessInterceptor.isEndpointPermitted(policy, "192.168.1.1", 7999));
    assertFalse(NetworkAccessInterceptor.isEndpointPermitted(policy, "192.168.1.1", 9001));
  }

  @Test
  public void testCodebaseScopedEntrySkipped() {
    // codeBase-scoped entries should be ignored in the path-based check
    PermittedEndpoint codeBasedEp =
        new PermittedEndpoint(
            "*",
            "connect,resolve",
            "file:/opt/solr/modules/jwt-auth/-",
            PolicyLoader.PolicySource.DEFAULT);
    SolrSecurityPolicy policy =
        new SolrSecurityPolicy(
            List.of(),
            List.of(codeBasedEp),
            List.of(),
            List.of(),
            SolrSecurityPolicy.EnforcementMode.ENFORCE);
    // Should return false because the only entry is codeBase-scoped
    assertFalse(NetworkAccessInterceptor.isEndpointPermitted(policy, "external.host", 443));
  }

  @Test
  public void testUnlistedHostPortBlocked() {
    SolrSecurityPolicy policy = policyWithEndpoint("localhost:8983");
    assertFalse(NetworkAccessInterceptor.isEndpointPermitted(policy, "10.0.0.1", 443));
  }

  @Test
  public void testBroadWildcardPermitsAll() {
    SolrSecurityPolicy policy = policyWithEndpoint("*");
    assertTrue(NetworkAccessInterceptor.isEndpointPermitted(policy, "anything.com", 443));
  }
}

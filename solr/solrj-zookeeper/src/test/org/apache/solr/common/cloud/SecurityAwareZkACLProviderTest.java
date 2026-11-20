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
package org.apache.solr.common.cloud;

import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/** Tests which znodes {@link SecurityAwareZkACLProvider} treats as security-related. */
public class SecurityAwareZkACLProviderTest extends SolrTestCase {
  private static final List<ACL> SECURITY =
      List.of(new ACL(ZooDefs.Perms.ALL, new Id("digest", "admin")));
  private static final List<ACL> NON_SECURITY =
      List.of(new ACL(ZooDefs.Perms.READ, new Id("digest", "readonly")));

  private static SecurityAwareZkACLProvider provider() {
    return new SecurityAwareZkACLProvider() {
      @Override
      protected List<ACL> createNonSecurityACLsToAdd() {
        return NON_SECURITY;
      }

      @Override
      protected List<ACL> createSecurityACLsToAdd() {
        return SECURITY;
      }
    };
  }

  /** Without a chroot, Solr's security znodes live at the ZooKeeper root. */
  public void testSecurityZnodesWithoutChroot() {
    SecurityAwareZkACLProvider provider = provider();
    assertEquals(SECURITY, provider.getACLsToAdd("/security.json"));
    assertEquals(SECURITY, provider.getACLsToAdd("/security"));
    assertEquals(SECURITY, provider.getACLsToAdd("/security/plugin"));
    assertEquals(NON_SECURITY, provider.getACLsToAdd("/"));
    assertEquals(NON_SECURITY, provider.getACLsToAdd("/collections"));
    assertEquals(NON_SECURITY, provider.getACLsToAdd("/securityfoo"));
  }

  public void testSecurityZnodesWithChroot() {
    SecurityAwareZkACLProvider provider = provider().withChroot("/solr");
    assertEquals(SECURITY, provider.getACLsToAdd("/solr/security.json"));
    assertEquals(SECURITY, provider.getACLsToAdd("/solr/security/plugin"));
    assertEquals(NON_SECURITY, provider.getACLsToAdd("/solr/collections"));
    assertEquals(NON_SECURITY, provider.getACLsToAdd("/security.json"));
  }
}

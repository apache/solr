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
package org.apache.solr.core;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.junit.Before;
import org.junit.Test;

/** Tests the ZooKeeper node role alongside the existing node roles. */
public class NodeRolesTest extends SolrTestCase {
  @Before
  public void disableEmbeddedZooKeeper() {
    System.setProperty("solr.zookeeper.server.enabled", "false");
  }

  @Test
  public void testZooKeeperRoleDefaultsToOff() {
    NodeRoles roles = new NodeRoles(null);
    assertEquals(NodeRoles.MODE_OFF, roles.getRoleMode(NodeRoles.Role.ZOOKEEPER));
    assertEquals(NodeRoles.MODE_ON, roles.getRoleMode(NodeRoles.Role.DATA));
    assertEquals(NodeRoles.MODE_ALLOWED, roles.getRoleMode(NodeRoles.Role.OVERSEER));
  }

  @Test
  public void testZooKeeperRolePreservesOtherRoles() {
    System.setProperty("solr.zookeeper.server.enabled", "true");
    NodeRoles roles = new NodeRoles("data:off,overseer:preferred,zookeeper:off");
    assertEquals(NodeRoles.MODE_ON, roles.getRoleMode(NodeRoles.Role.ZOOKEEPER));
    assertEquals(NodeRoles.MODE_OFF, roles.getRoleMode(NodeRoles.Role.DATA));
    assertEquals(NodeRoles.MODE_PREFERRED, roles.getRoleMode(NodeRoles.Role.OVERSEER));
  }

  @Test
  public void testEmbeddedZooKeeperKeepsDefaultRoles() {
    System.setProperty("solr.zookeeper.server.enabled", "true");
    NodeRoles roles = new NodeRoles(null);
    assertEquals(NodeRoles.MODE_ON, roles.getRoleMode(NodeRoles.Role.ZOOKEEPER));
    assertEquals(NodeRoles.MODE_ON, roles.getRoleMode(NodeRoles.Role.DATA));
    assertEquals(NodeRoles.MODE_ALLOWED, roles.getRoleMode(NodeRoles.Role.OVERSEER));
  }

  @Test
  public void testZooKeeperRoleIgnoresExplicitValue() {
    // The zookeeper role always reflects solr.zookeeper.server.enabled (false here, via
    // @Before), regardless of what the roles string explicitly requests.
    assertEquals(
        NodeRoles.MODE_OFF,
        new NodeRoles("data:on,zookeeper:on").getRoleMode(NodeRoles.Role.ZOOKEEPER));
    assertEquals(
        NodeRoles.MODE_OFF,
        new NodeRoles("data:on,zookeeper:off").getRoleMode(NodeRoles.Role.ZOOKEEPER));
  }

  @Test
  public void testZooKeeperRoleRejectsUnsupportedMode() {
    SolrException error =
        assertThrows(SolrException.class, () -> new NodeRoles("zookeeper:preferred"));
    assertTrue(error.getMessage().contains("Unknown role mode"));
  }
}

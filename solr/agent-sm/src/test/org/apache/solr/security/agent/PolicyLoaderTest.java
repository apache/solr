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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Unit tests for {@link PolicyLoader} — policy parsing, variable substitution, and merging. */
public class PolicyLoaderTest extends SolrTestCase {

  // ---------------------------------------------------------------------------
  // Variable expansion (PolicyPropertyExpander)
  // ---------------------------------------------------------------------------

  @Test
  public void testExpandKnownSystemProperty() throws Exception {
    System.setProperty("solr.solr.home", "/opt/solr");
    assertEquals("/opt/solr", PolicyPropertyExpander.expand("${solr.solr.home}"));
  }

  @Test
  public void testExpandSolrPort() throws Exception {
    System.setProperty("solr.port.listen", "8983");
    assertEquals("*:8983", PolicyPropertyExpander.expand("*:${solr.port.listen}"));
  }

  @Test
  public void testSolrZkPortDefaultsToSolrPortPlusOneThousand() throws Exception {
    System.setProperty("solr.port.listen", "8983");
    System.clearProperty("solr.zk.port");
    assertEquals("*:9983", PolicyPropertyExpander.expand("*:${solr.zk.port}"));
  }

  @Test
  public void testSolrZkPortExplicitOverride() throws Exception {
    System.setProperty("solr.zk.port", "2181");
    try {
      assertEquals("*:2181", PolicyPropertyExpander.expand("*:${solr.zk.port}"));
    } finally {
      System.clearProperty("solr.zk.port");
    }
  }

  @Test
  public void testExpandNullReturnsNull() throws Exception {
    assertNull(PolicyPropertyExpander.expand(null));
  }

  @Test(expected = PolicyPropertyExpander.ExpandException.class)
  public void testUnknownVariableThrows() throws Exception {
    PolicyPropertyExpander.expand("path=${solr.this.property.does.not.exist.xyz}");
  }

  // ---------------------------------------------------------------------------
  // Policy block parsing
  // ---------------------------------------------------------------------------

  @Test
  public void testGlobalGrantFilePermissionParsed() {
    String policy =
        "grant {\n" + "  permission java.io.FilePermission \"/solr/home/-\", \"read\";\n" + "};";
    List<PolicyLoader.GrantBlock> blocks = new ArrayList<>();
    PolicyLoader.parsePolicyBlocks(policy, PolicyLoader.PolicySource.DEFAULT, blocks);

    assertEquals(1, blocks.size());
    PolicyLoader.GrantBlock block = blocks.get(0);
    assertNull(block.codeBase);
    assertEquals(1, block.filePaths.size());
    assertEquals("/solr/home/-", block.filePaths.get(0).target);
    assertEquals("read", block.filePaths.get(0).actions);
    assertEquals(PolicyLoader.PolicySource.DEFAULT, block.filePaths.get(0).source);
  }

  @Test
  public void testGlobalGrantSocketPermissionParsed() {
    String policy =
        "grant {\n"
            + "  permission java.net.SocketPermission \"*:8983\", \"connect,resolve\";\n"
            + "};";
    List<PolicyLoader.GrantBlock> blocks = new ArrayList<>();
    PolicyLoader.parsePolicyBlocks(policy, PolicyLoader.PolicySource.DEFAULT, blocks);

    assertEquals(1, blocks.size());
    assertEquals(1, blocks.get(0).socketPerms.size());
    assertEquals("*:8983", blocks.get(0).socketPerms.get(0).hostPort);
    assertNull(blocks.get(0).socketPerms.get(0).codeBase);
  }

  @Test
  public void testCodeBaseScopedGrantParsed() {
    String policy =
        "grant codeBase \"file:/opt/solr/modules/jwt-auth/-\" {\n"
            + "  permission java.net.SocketPermission \"*\", \"connect,resolve\";\n"
            + "};";
    List<PolicyLoader.GrantBlock> blocks = new ArrayList<>();
    PolicyLoader.parsePolicyBlocks(policy, PolicyLoader.PolicySource.DEFAULT, blocks);

    assertEquals(1, blocks.size());
    PolicyLoader.GrantBlock block = blocks.get(0);
    assertEquals("file:/opt/solr/modules/jwt-auth/-", block.codeBase);
    assertEquals(1, block.socketPerms.size());
    assertEquals("*", block.socketPerms.get(0).hostPort);
    assertEquals("file:/opt/solr/modules/jwt-auth/-", block.socketPerms.get(0).codeBase);
  }

  @Test
  public void testLineCommentsStripped() {
    String policy =
        "grant { // this is a comment\n"
            + "  // another comment\n"
            + "  permission java.io.FilePermission \"/tmp/-\", \"read,write,delete\";\n"
            + "};";
    List<PolicyLoader.GrantBlock> blocks = new ArrayList<>();
    PolicyLoader.parsePolicyBlocks(policy, PolicyLoader.PolicySource.DEFAULT, blocks);

    assertEquals(1, blocks.size());
    assertEquals(1, blocks.get(0).filePaths.size());
  }

  @Test
  public void testUnrecognisedPermissionTypeIgnored() {
    String policy =
        "grant {\n"
            + "  permission java.util.PropertyPermission \"*\", \"read\";\n"
            + "  permission java.io.FilePermission \"/tmp/-\", \"read\";\n"
            + "};";
    List<PolicyLoader.GrantBlock> blocks = new ArrayList<>();
    PolicyLoader.parsePolicyBlocks(policy, PolicyLoader.PolicySource.DEFAULT, blocks);

    assertEquals(1, blocks.size());
    assertEquals(1, blocks.get(0).filePaths.size());
    // PropertyPermission is silently ignored
  }

  // ---------------------------------------------------------------------------
  // Full load — file I/O
  // ---------------------------------------------------------------------------

  @Test
  public void testValidPolicyParsesCorrectly() throws Exception {
    Path policyFile = createTempFile("agent-security", ".policy");
    Files.writeString(
        policyFile,
        "grant {\n"
            + "  permission java.io.FilePermission \"/solr/home/-\", \"read\";\n"
            + "  permission java.net.SocketPermission \"localhost:8983\", \"connect,resolve\";\n"
            + "};\n",
        StandardCharsets.UTF_8);

    PolicyLoader loader = new PolicyLoader();
    AgentPolicy policy = loader.load(policyFile);

    assertFalse(policy.permittedPaths().isEmpty());
    assertFalse(policy.permittedEndpoints().isEmpty());
  }

  @Test(expected = IllegalStateException.class)
  public void testMissingDefaultPolicyThrowsOnLoad() throws Exception {
    Path missing = createTempDir().resolve("no-such-policy.policy");
    new PolicyLoader().load(missing);
  }

  @Test
  public void testExtraPolicyAbsentIsNonFatal() throws Exception {
    Path defaultPolicy = createTempFile("agent-security", ".policy");
    Files.writeString(
        defaultPolicy,
        "grant { permission java.io.FilePermission \"/solr/home/-\", \"read\"; };\n",
        StandardCharsets.UTF_8);

    // Point extra-policy at a non-existent file
    System.setProperty(
        "solr.security.agent.extra.policy", createTempDir().resolve("absent.policy").toString());
    try {
      AgentPolicy policy = new PolicyLoader().load(defaultPolicy);
      // Must succeed — absent extra policy is not an error
      assertNotNull(policy);
    } finally {
      System.clearProperty("solr.security.agent.extra.policy");
    }
  }

  @Test
  public void testExtraPolicyMergedAndTaggedOperator() throws Exception {
    Path defaultPolicy = createTempFile("default", ".policy");
    Files.writeString(
        defaultPolicy,
        "grant { permission java.io.FilePermission \"/solr/home/-\", \"read\"; };\n",
        StandardCharsets.UTF_8);

    Path extraPolicy = createTempFile("extra", ".policy");
    Files.writeString(
        extraPolicy,
        "grant { permission java.io.FilePermission \"/mnt/nfs/-\", \"read\"; };\n",
        StandardCharsets.UTF_8);

    System.setProperty("solr.security.agent.extra.policy", extraPolicy.toString());
    try {
      AgentPolicy policy = new PolicyLoader().load(defaultPolicy);

      // Both paths must be present
      List<PermittedPath> paths = policy.permittedPaths();
      assertTrue(paths.stream().anyMatch(p -> p.path().equals("/solr/home")));
      assertTrue(paths.stream().anyMatch(p -> p.path().equals("/mnt/nfs")));

      // Operator path carries OPERATOR source tag
      PermittedPath operatorPath =
          paths.stream().filter(p -> p.path().equals("/mnt/nfs")).findFirst().orElseThrow();
      assertEquals(PolicyLoader.PolicySource.OPERATOR, operatorPath.source());
    } finally {
      System.clearProperty("solr.security.agent.extra.policy");
    }
  }

  @Test
  public void testRecursivePathFlagSet() throws Exception {
    Path policyFile = createTempFile("agent-security", ".policy");
    Files.writeString(
        policyFile,
        "grant { permission java.io.FilePermission \"/data/-\", \"read,write,delete\"; };\n",
        StandardCharsets.UTF_8);

    AgentPolicy policy = new PolicyLoader().load(policyFile);
    PermittedPath path = policy.permittedPaths().get(0);
    assertEquals("/data", path.path());
    assertTrue(path.recursive());
  }
}

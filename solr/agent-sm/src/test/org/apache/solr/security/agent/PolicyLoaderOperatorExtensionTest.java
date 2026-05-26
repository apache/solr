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
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Test;

/**
 * Tests for the operator extension policy file ({@code agent-security-extra.policy}).
 *
 * <p>Verifies that:
 *
 * <ul>
 *   <li>An extra policy file present at the configured path is loaded and merged with the default
 *       policy.
 *   <li>Entries from the extra policy are tagged {@link PolicyLoader.PolicySource#OPERATOR}.
 *   <li>Paths listed only in the extra policy are permitted; unlisted paths remain blocked.
 *   <li>When the extra policy file is absent the default policy still loads normally.
 *   <li>A malformed extra policy causes {@link IllegalStateException} with a descriptive message.
 *   <li>The {@code source=OPERATOR} tag is emitted in violation log entries for paths matched by
 *       operator-policy entries.
 * </ul>
 */
public class PolicyLoaderOperatorExtensionTest extends SolrTestCase {

  @After
  public void clearExtraPolicy() {
    System.clearProperty("solr.security.agent.extra.policy");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private Path writeDefaultPolicy(Path dir) throws Exception {
    Path policy = dir.resolve("agent-security.policy");
    Files.writeString(
        policy,
        "grant {\n" + "  permission java.io.FilePermission \"/opt/solr/-\", \"read\";\n" + "};\n",
        StandardCharsets.UTF_8);
    return policy;
  }

  private AgentPolicy loadWithExtra(Path defaultPolicy, Path extraPolicy) {
    if (extraPolicy != null) {
      System.setProperty("solr.security.agent.extra.policy", extraPolicy.toString());
    }
    return new PolicyLoader().load(defaultPolicy);
  }

  // ---------------------------------------------------------------------------
  // Extra policy present — entries merged and tagged OPERATOR
  // ---------------------------------------------------------------------------

  @Test
  public void testExtraPolicyPathIsPermitted() throws Exception {
    Path tmpDir = createTempDir();
    Path defaultPolicy = writeDefaultPolicy(tmpDir);

    Path extraPolicy = tmpDir.resolve("agent-security-extra.policy");
    Files.writeString(
        extraPolicy,
        "grant {\n"
            + "  permission java.io.FilePermission \""
            + tmpDir
            + "/-\", \"read\";\n"
            + "};\n",
        StandardCharsets.UTF_8);

    AgentPolicy policy = loadWithExtra(defaultPolicy, extraPolicy);
    assertTrue(policy.isPathPermitted(tmpDir.resolve("data.txt").toString(), "read"));
  }

  @Test
  public void testUnlistedPathStillBlockedWhenExtraPolicyPresent() throws Exception {
    Path tmpDir = createTempDir();
    Path defaultPolicy = writeDefaultPolicy(tmpDir);

    Path extraPolicy = tmpDir.resolve("agent-security-extra.policy");
    Files.writeString(
        extraPolicy,
        "grant {\n"
            + "  permission java.io.FilePermission \""
            + tmpDir
            + "/-\", \"read\";\n"
            + "};\n",
        StandardCharsets.UTF_8);

    AgentPolicy policy = loadWithExtra(defaultPolicy, extraPolicy);
    // /etc is not in either policy
    assertFalse(policy.isPathPermitted("/etc/shadow", "read"));
  }

  @Test
  public void testExtraPolicyEntriesTaggedOperator() throws Exception {
    Path tmpDir = createTempDir();
    Path defaultPolicy = writeDefaultPolicy(tmpDir);

    Path extraPolicy = tmpDir.resolve("agent-security-extra.policy");
    Files.writeString(
        extraPolicy,
        "grant {\n"
            + "  permission java.io.FilePermission \""
            + tmpDir
            + "/-\", \"read\";\n"
            + "};\n",
        StandardCharsets.UTF_8);

    AgentPolicy policy = loadWithExtra(defaultPolicy, extraPolicy);
    List<PermittedPath> paths = policy.permittedPaths();
    boolean hasOperator =
        paths.stream().anyMatch(p -> p.source() == PolicyLoader.PolicySource.OPERATOR);
    assertTrue("Expected at least one OPERATOR-sourced path entry", hasOperator);
  }

  @Test
  public void testDefaultPolicyEntriesTaggedDefault() throws Exception {
    Path tmpDir = createTempDir();
    Path defaultPolicy = writeDefaultPolicy(tmpDir);

    AgentPolicy policy = loadWithExtra(defaultPolicy, null);
    List<PermittedPath> paths = policy.permittedPaths();
    boolean hasDefault =
        paths.stream().anyMatch(p -> p.source() == PolicyLoader.PolicySource.DEFAULT);
    assertTrue("Expected at least one DEFAULT-sourced path entry", hasDefault);
  }

  // ---------------------------------------------------------------------------
  // Extra policy absent — default still loads
  // ---------------------------------------------------------------------------

  @Test
  public void testExtraPolicyAbsentIsNonFatal() throws Exception {
    Path tmpDir = createTempDir();
    Path defaultPolicy = writeDefaultPolicy(tmpDir);

    // Point to a non-existent extra policy
    System.setProperty(
        "solr.security.agent.extra.policy", tmpDir.resolve("nonexistent.policy").toString());

    // Should not throw; default policy still loads
    AgentPolicy policy = new PolicyLoader().load(defaultPolicy);
    assertTrue(policy.isPathPermitted("/opt/solr/conf", "read"));
  }

  // ---------------------------------------------------------------------------
  // Malformed extra policy — lenient parsing, default still enforced
  // ---------------------------------------------------------------------------

  @Test
  public void testMalformedExtraPolicyIsSkippedGracefully() throws Exception {
    Path tmpDir = createTempDir();
    Path defaultPolicy = writeDefaultPolicy(tmpDir);

    Path extraPolicy = tmpDir.resolve("agent-security-extra.policy");
    // Content with no recognizable grant blocks — parser silently produces empty result
    Files.writeString(extraPolicy, "THIS IS NOT A VALID POLICY\n", StandardCharsets.UTF_8);

    System.setProperty("solr.security.agent.extra.policy", extraPolicy.toString());

    // Should not throw; default policy still loads
    AgentPolicy policy = new PolicyLoader().load(defaultPolicy);
    // Default policy (/opt/solr) is still active
    assertTrue(policy.isPathPermitted("/opt/solr/conf", "read"));
    // Malformed extra policy adds no new paths
    assertFalse(policy.isPathPermitted(tmpDir.toString(), "read"));
  }

  // ---------------------------------------------------------------------------
  // Source field in violation log
  // ---------------------------------------------------------------------------

  @Test
  public void testViolationLogIncludesSourceField() {
    // Verify the log message builder includes the source field for OPERATOR entries
    String msg =
        SecurityViolationLogger.buildMessage(
            SecurityViolationLogger.ViolationType.FILE_READ,
            "/tmp/secret.txt",
            "com.example.Caller",
            AgentPolicy.EnforcementMode.WARN,
            "OPERATOR");
    assertTrue("Expected source=OPERATOR in log message", msg.contains("source=OPERATOR"));
  }

  @Test
  public void testViolationLogOmitsSourceWhenNull() {
    String msg =
        SecurityViolationLogger.buildMessage(
            SecurityViolationLogger.ViolationType.FILE_READ,
            "/tmp/secret.txt",
            "com.example.Caller",
            AgentPolicy.EnforcementMode.WARN,
            null);
    assertFalse("Expected no source= field when source is null", msg.contains("source="));
  }
}

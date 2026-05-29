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

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Reads and parses JDK-style {@code .policy} files, performing Solr-specific variable substitution,
 * and produces a {@link AgentPolicy} ready for enforcement.
 *
 * <h2>Variable substitution</h2>
 *
 * {@code ${property}} placeholders in permission targets and {@code codeBase} URLs are expanded
 * per-token by {@link PolicyPropertyExpander} using system properties. Any unresolved placeholder
 * causes startup to fail immediately (fail-fast). The only built-in default is {@code
 * ${solr.zk.port}}, which falls back to {@code solr.port + 1000} when not explicitly set.
 *
 * <h2>Two-file merge</h2>
 *
 * Two files are loaded: the mandatory default policy and an optional operator extension file. The
 * extra-policy file path is resolved from system property {@code solr.security.agent.extra.policy},
 * falling back to {@code ${server.dir}/etc/agent-security-extra.policy}. An absent extra-policy
 * file is silently skipped. Each entry carries a {@link PermittedPath#source() source} tag of
 * either {@link PolicySource#DEFAULT} or {@link PolicySource#OPERATOR}.
 *
 * <p>A missing or unparseable <em>default</em> policy causes an {@link IllegalStateException} at
 * startup.
 */
public class PolicyLoader {

  /**
   * Loads and merges the default policy file and the optional operator extension file.
   *
   * @param defaultPolicyPath absolute path to the default {@code agent-security.policy} file
   * @return a fully initialized {@link AgentPolicy}
   * @throws IllegalStateException if the default policy file is absent or cannot be parsed
   */
  public AgentPolicy load(Path defaultPolicyPath) {
    if (!Files.exists(defaultPolicyPath)) {
      throw new IllegalStateException(
          "Security agent default policy not found: "
              + defaultPolicyPath
              + ". Solr cannot start without a valid security policy. "
              + "Check that agent-security.policy is present in server/etc/.");
    }

    String defaultContent;
    try {
      defaultContent = Files.readString(defaultPolicyPath, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read security agent default policy: " + defaultPolicyPath, e);
    }

    List<GrantBlock> grants = new ArrayList<>();
    parsePolicy(defaultContent, PolicySource.DEFAULT, grants);
    if (grants.isEmpty()) {
      throw new IllegalStateException(
          "Security agent default policy contains no grant blocks: "
              + defaultPolicyPath
              + ". The default policy must define at least one grant.");
    }

    // Resolve extra-policy path: system property → fallback to ${server.dir}/etc/...
    Path extraPolicyPath = resolveExtraPolicyPath();
    if (extraPolicyPath != null && Files.exists(extraPolicyPath)) {
      String extraContent;
      try {
        extraContent = Files.readString(extraPolicyPath, StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to read operator security policy extension: " + extraPolicyPath, e);
      }
      int beforeCount = grants.size();
      parsePolicy(extraContent, PolicySource.OPERATOR, grants);
      if (grants.size() == beforeCount) {
        agentOut(
            "[Solr SecurityAgent] Operator extension policy is empty (no grant blocks): "
                + extraPolicyPath);
      }
    }

    return buildPolicy(grants);
  }

  /**
   * Resolves the extra-policy file path from system property {@code
   * solr.security.agent.extra.policy}, falling back to {@code
   * ${server.dir}/etc/agent-security-extra.policy}. Returns {@code null} if no fallback is
   * available.
   */
  static Path resolveExtraPolicyPath() {
    String explicitPath =
        PolicyPropertyExpander.getPropertyOrEnv("solr.security.agent.extra.policy");
    if (explicitPath != null && !explicitPath.isBlank()) {
      return Path.of(explicitPath);
    }
    String serverDir = System.getProperty("jetty.home", System.getProperty("server.dir"));
    if (serverDir != null && !serverDir.isBlank()) {
      return Path.of(serverDir, "etc", "agent-security-extra.policy");
    }
    return null;
  }

  /**
   * Parses a policy file and appends the resulting {@link GrantBlock} entries — tagged with the
   * given {@code source} — to {@code out}. Variable substitution is performed per-token by {@link
   * PolicyPropertyExpander}; any unresolved {@code ${variable}} causes an {@link
   * IllegalStateException}.
   */
  static void parsePolicy(String content, PolicySource source, List<GrantBlock> out) {
    parsePolicyBlocks(content, source, out);
  }

  /**
   * Parses grant blocks from the given (already variable-substituted) policy text. Only the
   * permission types used by the Solr agent are recognised:
   *
   * <ul>
   *   <li>{@code java.io.FilePermission} → {@link PermittedPath}
   *   <li>{@code java.net.SocketPermission} → {@link PermittedEndpoint}
   *   <li>{@code java.lang.RuntimePermission "exitVM"} → {@link ApprovedCallSite} EXIT
   *   <li>{@code java.lang.RuntimePermission "exec"} → {@link ApprovedCallSite} EXEC
   * </ul>
   *
   * <p>Parsing uses {@link PolicyFileParser} (backed by {@link java.io.StreamTokenizer}) which
   * natively handles {@code //} and {@code /* *\/} comments and quoted strings — no regex.
   */
  static void parsePolicyBlocks(String text, PolicySource source, List<GrantBlock> out) {
    List<PolicyFileParser.GrantEntry> grantEntries;
    try {
      grantEntries = PolicyFileParser.read(new StringReader(text));
    } catch (PolicyFileParser.ParsingException | IOException e) {
      throw new IllegalStateException("Failed to parse security policy: " + e.getMessage(), e);
    }
    for (PolicyFileParser.GrantEntry ge : grantEntries) {
      GrantBlock block = new GrantBlock(ge.codeBase(), source);
      for (PolicyFileParser.PermEntry pe : ge.permissions()) {
        addPermission(pe, block);
      }
      out.add(block);
    }
  }

  private static void addPermission(PolicyFileParser.PermEntry pe, GrantBlock block) {
    String permClass = pe.permission();
    String target = pe.name();
    String actions = pe.action();
    switch (permClass) {
      case "java.io.FilePermission":
        if (target != null) {
          block.filePaths.add(
              new RawFilePermission(target, actions != null ? actions : "read", block.source));
        }
        break;
      case "java.net.SocketPermission":
        if (target != null) {
          block.socketPerms.add(
              new RawSocketPermission(
                  target,
                  actions != null ? actions : "connect,resolve",
                  block.codeBase,
                  block.source));
        }
        break;
      case "java.lang.RuntimePermission":
        if ("exitVM".equals(target) || (target != null && target.startsWith("exitVM."))) {
          block.runtimePerms.add(new RawRuntimePermission("exitVM", block.codeBase, block.source));
        } else if ("exec".equals(target)) {
          block.runtimePerms.add(new RawRuntimePermission("exec", block.codeBase, block.source));
        }
        break;
      default:
        // Unrecognised permission types are ignored (e.g. PropertyPermission in legacy policy)
        break;
    }
  }

  /** Converts raw parsed grant blocks into an immutable {@link AgentPolicy}. */
  private AgentPolicy buildPolicy(List<GrantBlock> grants) {
    List<PermittedPath> paths = new ArrayList<>();
    List<PermittedEndpoint> endpoints = new ArrayList<>();
    List<ApprovedCallSite> exitCallers = new ArrayList<>();
    List<ApprovedCallSite> execCallers = new ArrayList<>();

    for (GrantBlock block : grants) {
      for (RawFilePermission fp : block.filePaths) {
        boolean recursive = fp.target.endsWith("/-") || fp.target.endsWith("\\-");
        String basePath = recursive ? fp.target.substring(0, fp.target.length() - 2) : fp.target;
        // Normalize the policy path using the same resolution strategy as FileInterceptor so that
        // comparisons are always apples-to-apples. toRealPath() resolves symlinks; fall back to
        // toAbsolutePath().normalize() when the path does not exist yet or the Old Java
        // SecurityManager blocks the read check on the resolved path.
        try {
          basePath = Path.of(basePath).toRealPath().toString();
        } catch (IOException | SecurityException e) {
          try {
            basePath = Path.of(basePath).toAbsolutePath().normalize().toString();
          } catch (Exception ignored) {
            // keep the variable-substituted string as-is
          }
        }
        paths.add(new PermittedPath(basePath, fp.actions, recursive, fp.source));
      }
      for (RawSocketPermission sp : block.socketPerms) {
        endpoints.add(new PermittedEndpoint(sp.hostPort, sp.actions, sp.codeBase, sp.source));
      }
      for (RawRuntimePermission rp : block.runtimePerms) {
        if ("exitVM".equals(rp.type)) {
          // Grant without codeBase: "*" (any class may exit); with codeBase: match by code source
          exitCallers.add(
              new ApprovedCallSite(
                  rp.codeBase != null ? null : "*",
                  rp.codeBase,
                  ApprovedCallSite.Operation.EXIT,
                  rp.source));
        } else if ("exec".equals(rp.type)) {
          execCallers.add(
              new ApprovedCallSite(
                  rp.codeBase != null ? null : "*",
                  rp.codeBase,
                  ApprovedCallSite.Operation.EXEC,
                  rp.source));
        }
      }
    }

    // Read enforcement mode from sysprop or env var (agent has no dep on solr:core/EnvUtils)
    String modeStr = PolicyPropertyExpander.getPropertyOrEnv("solr.security.agent.mode");
    if (modeStr == null) modeStr = "warn";
    AgentPolicy.EnforcementMode mode =
        "enforce".equalsIgnoreCase(modeStr.trim())
            ? AgentPolicy.EnforcementMode.ENFORCE
            : AgentPolicy.EnforcementMode.WARN;

    // 0.0.0.0 is the unspecified bind address, not a loopback — intentionally excluded.
    Set<String> trustedHosts = Set.of("localhost", "127.0.0.1", "0:0:0:0:0:0:0:1", "::1");
    // jar/zip/jrt are internal JVM class-loading file systems. Intercepting them causes a
    // class-initialization deadlock: violation logger → SLF4J → Log4j2 init → JAR read → repeat.
    Set<String> trustedFileSystems = Set.of("jar", "zip", "jrt");
    return new AgentPolicy(
        paths, endpoints, exitCallers, execCallers, mode, trustedFileSystems, trustedHosts);
  }

  // ---------------------------------------------------------------------------
  // Internal data transfer objects for parsed policy entries
  // ---------------------------------------------------------------------------

  /** Holds the parsed contents of one grant { } block. */
  static class GrantBlock {
    final String codeBase; // null for global grants
    final PolicySource source;
    final List<RawFilePermission> filePaths = new ArrayList<>();
    final List<RawSocketPermission> socketPerms = new ArrayList<>();
    final List<RawRuntimePermission> runtimePerms = new ArrayList<>();

    GrantBlock(String codeBase, PolicySource source) {
      this.codeBase = codeBase;
      this.source = source;
    }
  }

  static class RawFilePermission {
    final String target;
    final String actions;
    final PolicySource source;

    RawFilePermission(String target, String actions, PolicySource source) {
      this.target = target;
      this.actions = actions;
      this.source = source;
    }
  }

  static class RawSocketPermission {
    final String hostPort;
    final String actions;
    final String codeBase;
    final PolicySource source;

    RawSocketPermission(String hostPort, String actions, String codeBase, PolicySource source) {
      this.hostPort = hostPort;
      this.actions = actions;
      this.codeBase = codeBase;
      this.source = source;
    }
  }

  static class RawRuntimePermission {
    final String type; // "exitVM" or "exec"
    final String codeBase;
    final PolicySource source;

    RawRuntimePermission(String type, String codeBase, PolicySource source) {
      this.type = type;
      this.codeBase = codeBase;
      this.source = source;
    }
  }

  @SuppressForbidden(
      reason =
          "System.err is the only safe output channel during premain, before SLF4J is available.")
  private static void agentOut(String msg) {
    System.err.println(msg);
  }
}

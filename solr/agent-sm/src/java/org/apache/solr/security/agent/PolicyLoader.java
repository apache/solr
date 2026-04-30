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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads and parses JDK-style {@code .policy} files, performing Solr-specific variable substitution,
 * and produces a {@link SolrSecurityPolicy} ready for enforcement.
 *
 * <h2>Variable substitution</h2>
 *
 * The following variables are expanded in permission targets and {@code codeBase} URLs before
 * parsing:
 *
 * <ul>
 *   <li>{@code ${solr.home}} — Solr home directory
 *   <li>{@code ${solr.data.dir}} — Solr data directory
 *   <li>{@code ${solr.log.dir}} — Solr log directory
 *   <li>{@code ${solr.install.dir}} — Solr installation root (parent of {@code server/})
 *   <li>{@code ${java.io.tmpdir}} — JVM temporary directory
 *   <li>{@code ${java.home}} — JDK installation directory
 *   <li>{@code ${user.home}} — OS user home directory
 *   <li>{@code ${solr.port}} — Solr HTTP port (from system property {@code solr.port})
 *   <li>{@code ${solr.zk.port}} — ZooKeeper port; defaults to {@code solr.port + 1000} when not
 *       explicitly configured
 * </ul>
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

  /** The source that an entry came from — default bundled policy or operator extension. */
  public enum PolicySource {
    DEFAULT,
    OPERATOR
  }

  // Variables expanded in path and codeBase expressions before parsing.
  private static final String[] SYSTEM_VARS = {
    "solr.home",
    "solr.data.dir",
    "solr.log.dir",
    "solr.install.dir",
    "java.io.tmpdir",
    "java.home",
    "user.home",
  };

  // Pattern to match a variable reference such as ${solr.home}
  private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

  /**
   * Loads and merges the default policy file and the optional operator extension file.
   *
   * @param defaultPolicyPath absolute path to the default {@code agent-security.policy} file
   * @return a fully initialized {@link SolrSecurityPolicy}
   * @throws IllegalStateException if the default policy file is absent or cannot be parsed
   */
  public SolrSecurityPolicy load(Path defaultPolicyPath) {
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
      parsePolicy(extraContent, PolicySource.OPERATOR, grants);
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
    String explicitPath = System.getProperty("solr.security.agent.extra.policy");
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
   * Parses a policy file, substituting variables, and appends the resulting {@link GrantBlock}
   * entries — tagged with the given {@code source} — to {@code out}.
   */
  static void parsePolicy(String content, PolicySource source, List<GrantBlock> out) {
    String expanded = substituteVariables(content);
    parsePolicyBlocks(expanded, source, out);
  }

  /**
   * Expands all {@code ${variable}} references in {@code text}. Unknown variables are left as-is.
   * {@code ${solr.port}} and {@code ${solr.zk.port}} receive special handling so that the ZK port
   * defaults to {@code solr.port + 1000} when not explicitly set.
   */
  static String substituteVariables(String text) {
    // Resolve solr.port once so we can derive the default ZK port.
    String solrPortStr = System.getProperty("solr.port", "8983");
    int solrPort;
    try {
      solrPort = Integer.parseInt(solrPortStr.trim());
    } catch (NumberFormatException e) {
      solrPort = 8983;
    }
    String zkPortStr = System.getProperty("solr.zk.port");
    String zkPort =
        (zkPortStr != null && !zkPortStr.isBlank())
            ? zkPortStr.trim()
            : String.valueOf(solrPort + 1000);

    StringBuffer sb = new StringBuffer();
    Matcher m = VAR_PATTERN.matcher(text);
    while (m.find()) {
      String varName = m.group(1);
      String replacement;
      if ("solr.port".equals(varName)) {
        replacement = solrPortStr.trim();
      } else if ("solr.zk.port".equals(varName)) {
        replacement = zkPort;
      } else {
        replacement = resolveSystemVar(varName);
      }
      m.appendReplacement(
          sb, Matcher.quoteReplacement(replacement != null ? replacement : m.group(0)));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  private static String resolveSystemVar(String varName) {
    // Try system property first, then environment-style lookup.
    String val = System.getProperty(varName);
    if (val != null) return val;
    // For well-known vars, also try without dots.
    return null;
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
   */
  static void parsePolicyBlocks(String text, PolicySource source, List<GrantBlock> out) {
    // Strip single-line comments
    String noComments = stripComments(text);

    // Match: grant [codeBase "url"] { ... };
    Pattern grantPattern =
        Pattern.compile(
            "grant\\s*(?:codeBase\\s*\"([^\"]*?)\")?\\s*\\{([^}]*)\\}\\s*;",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    Matcher grantMatcher = grantPattern.matcher(noComments);
    while (grantMatcher.find()) {
      String codeBase = grantMatcher.group(1); // null for global grants
      String body = grantMatcher.group(2);
      GrantBlock block = new GrantBlock(codeBase, source);
      parsePermissions(body, block);
      out.add(block);
    }
  }

  private static String stripComments(String text) {
    // Remove // line comments; leave /* */ block comments as-is (not used in standard policy files)
    StringBuilder sb = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new StringReader(text))) {
      String line;
      while ((line = reader.readLine()) != null) {
        int commentIdx = line.indexOf("//");
        if (commentIdx >= 0) {
          line = line.substring(0, commentIdx);
        }
        sb.append(line).append('\n');
      }
    } catch (IOException e) {
      // StringReader never throws
      throw new AssertionError(e);
    }
    return sb.toString();
  }

  /**
   * Parses individual {@code permission} lines inside a grant block body and adds recognised
   * permissions to the block.
   */
  static void parsePermissions(String body, GrantBlock block) {
    // permission <class> ["target"] [, "actions"];
    Pattern permPattern =
        Pattern.compile(
            "permission\\s+(\\S+)\\s*(?:\"([^\"]*?)\")?\\s*(?:,\\s*\"([^\"]*?)\")?\\s*;",
            Pattern.CASE_INSENSITIVE);

    Matcher m = permPattern.matcher(body);
    while (m.find()) {
      String permClass = m.group(1);
      String target = m.group(2); // may be null
      String actions = m.group(3); // may be null

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
            block.runtimePerms.add(
                new RawRuntimePermission("exitVM", block.codeBase, block.source));
          } else if ("exec".equals(target)) {
            block.runtimePerms.add(new RawRuntimePermission("exec", block.codeBase, block.source));
          }
          break;
        default:
          // Unrecognised permission types are ignored (e.g. PropertyPermission in legacy policy)
          break;
      }
    }
  }

  /** Converts raw parsed grant blocks into an immutable {@link SolrSecurityPolicy}. */
  private SolrSecurityPolicy buildPolicy(List<GrantBlock> grants) {
    List<PermittedPath> paths = new ArrayList<>();
    List<PermittedEndpoint> endpoints = new ArrayList<>();
    List<ApprovedCallSite> exitCallers = new ArrayList<>();
    List<ApprovedCallSite> execCallers = new ArrayList<>();

    for (GrantBlock block : grants) {
      for (RawFilePermission fp : block.filePaths) {
        boolean recursive = fp.target.endsWith("/-") || fp.target.endsWith("\\-");
        String basePath = recursive ? fp.target.substring(0, fp.target.length() - 2) : fp.target;
        paths.add(new PermittedPath(basePath, fp.actions, recursive, fp.source));
      }
      for (RawSocketPermission sp : block.socketPerms) {
        endpoints.add(new PermittedEndpoint(sp.hostPort, sp.actions, sp.codeBase, sp.source));
      }
      for (RawRuntimePermission rp : block.runtimePerms) {
        if ("exitVM".equals(rp.type)) {
          // codeBase-scoped exitVM grants map to approved exit callers
          String pattern = rp.codeBase != null ? rp.codeBase : "*";
          exitCallers.add(
              new ApprovedCallSite(pattern, ApprovedCallSite.Operation.EXIT, rp.source));
        } else if ("exec".equals(rp.type)) {
          String pattern = rp.codeBase != null ? rp.codeBase : "*";
          execCallers.add(
              new ApprovedCallSite(pattern, ApprovedCallSite.Operation.EXEC, rp.source));
        }
      }
    }

    // Read enforcement mode from system property (not from EnvUtils — agent has no dep on
    // solr:core)
    String modeStr = System.getProperty("solr.security.agent.mode", "warn");
    SolrSecurityPolicy.EnforcementMode mode =
        "enforce".equalsIgnoreCase(modeStr.trim())
            ? SolrSecurityPolicy.EnforcementMode.ENFORCE
            : SolrSecurityPolicy.EnforcementMode.WARN;

    return new SolrSecurityPolicy(paths, endpoints, exitCallers, execCallers, mode);
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
}

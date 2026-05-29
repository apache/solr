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
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Immutable singleton holding the active security policy for the Solr JVM.
 *
 * <p>Loaded once at startup by {@link PolicyLoader}; replacing it throws {@link SecurityException}.
 * Stored as {@code static volatile} so it is visible to the bootstrap classloader used by the agent
 * JAR.
 */
public final class AgentPolicy {

  /** Whether violations block the operation or are merely logged. */
  public enum EnforcementMode {
    /** Violations are logged at WARN level; the operation is allowed to proceed. */
    WARN,
    /** Violations are logged at ERROR level and blocked with a {@link SecurityException}. */
    ENFORCE
  }

  // Singleton holder — set once at premain; never null after initialization.
  private static volatile AgentPolicy instance;

  private final List<PermittedPath> permittedPaths;
  private final List<PermittedEndpoint> permittedEndpoints;
  private final List<ApprovedCallSite> approvedExitCallers;
  private final List<ApprovedCallSite> approvedExecCallers;
  private final EnforcementMode enforcementMode;
  private final Set<String> trustedFileSystems;
  private final Set<String> trustedHosts;

  /** Constructs the policy. Called exclusively by {@link PolicyLoader#load(java.nio.file.Path)}. */
  AgentPolicy(
      List<PermittedPath> permittedPaths,
      List<PermittedEndpoint> permittedEndpoints,
      List<ApprovedCallSite> approvedExitCallers,
      List<ApprovedCallSite> approvedExecCallers,
      EnforcementMode enforcementMode) {
    this(
        permittedPaths,
        permittedEndpoints,
        approvedExitCallers,
        approvedExecCallers,
        enforcementMode,
        Set.of(),
        Set.of());
  }

  /**
   * Constructs the policy with explicit trusted filesystem schemes (e.g. {@code "jrt"}, {@code
   * "memory"}) and trusted hosts (e.g. loopback addresses).
   */
  AgentPolicy(
      List<PermittedPath> permittedPaths,
      List<PermittedEndpoint> permittedEndpoints,
      List<ApprovedCallSite> approvedExitCallers,
      List<ApprovedCallSite> approvedExecCallers,
      EnforcementMode enforcementMode,
      Set<String> trustedFileSystems,
      Set<String> trustedHosts) {
    this.permittedPaths = Collections.unmodifiableList(permittedPaths);
    this.permittedEndpoints = Collections.unmodifiableList(permittedEndpoints);
    this.approvedExitCallers = Collections.unmodifiableList(approvedExitCallers);
    this.approvedExecCallers = Collections.unmodifiableList(approvedExecCallers);
    this.enforcementMode = enforcementMode;
    this.trustedFileSystems = Collections.unmodifiableSet(trustedFileSystems);
    this.trustedHosts = Collections.unmodifiableSet(trustedHosts);
  }

  // ---------------------------------------------------------------------------
  // Singleton management
  // ---------------------------------------------------------------------------

  /**
   * Sets the global singleton policy. May only be called once; subsequent calls throw {@link
   * SecurityException}.
   */
  public static void initialize(AgentPolicy policy) {
    synchronized (AgentPolicy.class) {
      if (instance != null) {
        throw new SecurityException(
            "AgentPolicy has already been initialized and cannot be replaced. "
                + "This is a programming error; only SolrAgentEntryPoint.premain() should call initialize().");
      }
      instance = policy;
    }
  }

  /**
   * Returns the active global policy.
   *
   * @throws IllegalStateException if the policy has not yet been initialized
   */
  public static AgentPolicy getInstance() {
    AgentPolicy p = instance;
    if (p == null) {
      throw new IllegalStateException(
          "AgentPolicy has not been initialized. "
              + "Ensure the Solr security agent JAR is on the -javaagent: command-line.");
    }
    return p;
  }

  /** Returns {@code true} if the singleton has been initialized. */
  public static boolean isInitialized() {
    return instance != null;
  }

  /** Resets the singleton for tests; must not be called from production code. */
  static void resetForTesting() {
    instance = null;
  }

  // ---------------------------------------------------------------------------
  // Policy accessors
  // ---------------------------------------------------------------------------

  /** Permitted file-system paths derived from both the default policy and operator extensions. */
  public List<PermittedPath> permittedPaths() {
    return permittedPaths;
  }

  /** Permitted outbound network endpoints. */
  public List<PermittedEndpoint> permittedEndpoints() {
    return permittedEndpoints;
  }

  /** Classes approved to call {@code System.exit()} or {@code Runtime.halt()}. */
  public List<ApprovedCallSite> approvedExitCallers() {
    return approvedExitCallers;
  }

  /**
   * Classes approved to spawn child processes via {@code ProcessBuilder} or {@code Runtime.exec()}.
   */
  public List<ApprovedCallSite> approvedExecCallers() {
    return approvedExecCallers;
  }

  /** Current enforcement mode. */
  public EnforcementMode enforcementMode() {
    return enforcementMode;
  }

  /**
   * Filesystem scheme names that are exempt from path-based checks (e.g. in-memory filesystems used
   * in tests).
   */
  public Set<String> trustedFileSystems() {
    return trustedFileSystems;
  }

  /** Host strings exempt from outbound network checks (e.g. {@code "localhost"}). */
  public Set<String> trustedHosts() {
    return trustedHosts;
  }

  // ---------------------------------------------------------------------------
  // Policy checks (convenience helpers called by interceptors)
  // ---------------------------------------------------------------------------

  /** Returns {@code true} if the policy permits {@code action} on the resolved path. */
  public boolean isPathPermitted(String resolvedPath, String action) {
    for (PermittedPath p : permittedPaths) {
      if (p.permits(resolvedPath, action)) return true;
    }
    return false;
  }

  /** Returns {@code true} if {@code className} matches an approved exit call-site. */
  public boolean isExitApproved(String className) {
    for (ApprovedCallSite cs : approvedExitCallers) {
      // codeBase entries require a Class<?> object; skip them here
      if (cs.operation() == ApprovedCallSite.Operation.EXIT
          && cs.codeBase() == null
          && cs.matches(className)) return true;
    }
    return false;
  }

  /** Returns {@code true} if {@code className} matches an approved exec call-site. */
  public boolean isExecApproved(String className) {
    for (ApprovedCallSite cs : approvedExecCallers) {
      // codeBase entries require a Class<?> object; skip them here
      if (cs.operation() == ApprovedCallSite.Operation.EXEC
          && cs.codeBase() == null
          && cs.matches(className)) return true;
    }
    return false;
  }

  /**
   * Returns {@code true} if any class in {@code chain} is approved to call {@code System.exit()}
   * or {@code Runtime.halt()}. Any match anywhere in the chain is sufficient.
   */
  public boolean isChainThatCanExit(Collection<Class<?>> chain) {
    for (Class<?> cls : chain) {
      for (ApprovedCallSite cs : approvedExitCallers) {
        if (cs.operation() != ApprovedCallSite.Operation.EXIT) continue;
        if (cs.codeBase() != null ? cs.matchesCodeBase(cls) : cs.matches(cls.getName())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if any class in {@code chain} is approved to spawn child processes.
   * Same semantics as {@link #isChainThatCanExit(Collection)}.
   */
  public boolean isChainThatCanExec(Collection<Class<?>> chain) {
    for (Class<?> cls : chain) {
      for (ApprovedCallSite cs : approvedExecCallers) {
        if (cs.operation() != ApprovedCallSite.Operation.EXEC) continue;
        if (cs.codeBase() != null ? cs.matchesCodeBase(cls) : cs.matches(cls.getName())) {
          return true;
        }
      }
    }
    return false;
  }
}

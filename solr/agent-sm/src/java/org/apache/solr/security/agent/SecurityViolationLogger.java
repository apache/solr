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

import java.util.function.Consumer;

/**
 * Emits structured log entries for security policy violations detected by the Solr security agent.
 *
 * <h2>Log format</h2>
 *
 * <pre>{@code
 * [Solr SecurityAgent] SECURITY VIOLATION [TYPE] target=<t> caller=<c> mode=<m> source=<DEFAULT|OPERATOR>
 * }</pre>
 *
 * <p>In {@link AgentPolicy.EnforcementMode#WARN warn mode} entries are emitted and the operation
 * proceeds. In {@link AgentPolicy.EnforcementMode#ENFORCE enforce mode} the operation is blocked.
 *
 * <p>During early startup violations go to {@code System.err}. Once {@code
 * AgentViolationBridge.wire()} is called from {@code CoreContainer}, the {@link #reporter} bridge
 * routes them to {@code solr.log} at {@code WARN} level.
 */
public final class SecurityViolationLogger {

  /**
   * Bridge set by {@code AgentViolationBridge.wire()} once SLF4J/Log4j2 is available. {@code null}
   * until wired — violations fall back to {@code System.err}. {@code volatile} for safe
   * publication.
   */
  public static volatile Consumer<String> reporter = null;

  /** The operation types that can trigger a violation. */
  public enum ViolationType {
    FILE_READ,
    FILE_WRITE,
    FILE_DELETE,
    NETWORK_CONNECT,
    SYSTEM_EXIT,
    PROCESS_EXEC
  }

  private SecurityViolationLogger() {}

  /** Records a security violation. */
  @SuppressForbidden(
      reason =
          "System.err is the only output channel safe from classloader conflicts in a bootstrap "
              + "agent. SLF4J in Boot-Class-Path permanently poisons the JVM-wide SLF4J binding.")
  public static void log(
      ViolationType type,
      String target,
      String caller,
      AgentPolicy.EnforcementMode mode,
      String source) {

    String message = buildMessage(type, target, caller, mode, source);
    Consumer<String> r = reporter;
    if (r != null) {
      r.accept(message);
    } else {
      System.err.println("[Solr SecurityAgent] " + message);
    }
  }

  /** Convenience overload without a {@code source} field (used before policy source tagging). */
  public static void log(
      ViolationType type, String target, String caller, AgentPolicy.EnforcementMode mode) {
    log(type, target, caller, mode, null);
  }

  public static String buildMessage(
      ViolationType type,
      String target,
      String caller,
      AgentPolicy.EnforcementMode mode,
      String source) {

    StringBuilder sb = new StringBuilder();
    sb.append("SECURITY VIOLATION [")
        .append(type.name())
        .append("] target=")
        .append(target)
        .append(" caller=")
        .append(caller)
        .append(" mode=")
        .append(mode.name());
    if (source != null) {
      sb.append(" source=").append(source);
    }
    return sb.toString();
  }
}

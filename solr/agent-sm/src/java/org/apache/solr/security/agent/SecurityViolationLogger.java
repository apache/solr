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

import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits structured log entries for security policy violations detected by the Solr security agent.
 *
 * <h2>Log format</h2>
 *
 * <pre>{@code
 * SECURITY VIOLATION [TYPE] target=<t> caller=<c> mode=<m> source=<DEFAULT|OPERATOR>
 * }</pre>
 *
 * <p>In {@link AgentPolicy.EnforcementMode#WARN warn mode} entries are logged at {@code WARN} level
 * and the operation is allowed to proceed. In {@link AgentPolicy.EnforcementMode#ENFORCE enforce
 * mode} entries are logged at {@code ERROR} level and the operation must be blocked by the calling
 * interceptor.
 *
 * <p>The {@code source} field identifies whether the matching policy entry (if any) came from the
 * default bundled policy ({@code DEFAULT}) or from an operator extension ({@code OPERATOR}). For
 * violations where no entry matched at all, {@code source} is omitted.
 *
 * <h2>SLF4J and classloader boundary</h2>
 *
 * Agent classes may be injected into the bootstrap classloader, where SLF4J is not directly
 * accessible. The logger is obtained lazily via the context classloader to bridge this boundary. If
 * SLF4J is not yet available (early startup), violations are written to {@code System.err}.
 */
public final class SecurityViolationLogger {

  /** The operation types that can trigger a violation. */
  public enum ViolationType {
    FILE_READ,
    FILE_WRITE,
    FILE_DELETE,
    NETWORK_CONNECT,
    SYSTEM_EXIT,
    PROCESS_EXEC
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SecurityViolationLogger() {}

  /**
   * Records a security violation.
   *
   * @param type the category of the blocked/warned operation
   * @param target the resource that was targeted (path, host:port, or operation descriptor)
   * @param caller the fully-qualified name of the top non-JDK caller class
   * @param mode the current enforcement mode
   * @param source the policy source tag ({@code "DEFAULT"}, {@code "OPERATOR"}, or {@code null} if
   *     no entry matched)
   */
  public static void log(
      ViolationType type,
      String target,
      String caller,
      AgentPolicy.EnforcementMode mode,
      String source) {

    String message = buildMessage(type, target, caller, mode, source);

    if (mode == AgentPolicy.EnforcementMode.ENFORCE) {
      log.error(message);
    } else {
      log.warn(message);
    }
    // When DEBUG logging is enabled, emit the current call stack so the violation origin is visible
    // in logs. The RuntimeException is never thrown — it is a carrier for the stack trace only.
    if (log.isDebugEnabled()) {
      log.debug("Call stack at point of violation:", new RuntimeException("call stack"));
    }
  }

  /**
   * Convenience overload without a {@code source} field (used during early startup before policy
   * source tagging is available).
   */
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

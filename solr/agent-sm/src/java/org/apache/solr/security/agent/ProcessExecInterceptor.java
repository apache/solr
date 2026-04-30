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

import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy {@link Advice} interceptor for child process spawning.
 *
 * <p>Intercepts {@code ProcessBuilder.start()} and {@code Runtime.exec()} to enforce the {@link
 * SolrSecurityPolicy#approvedExecCallers()} list. By default, no call sites are approved in the
 * production policy (the list is empty), so all process-spawning attempts will be flagged unless an
 * operator explicitly adds an entry to {@code agent-security-extra.policy}.
 *
 * <p>This interceptor is not present in the OpenSearch {@code agent-sm} module; it is a Solr
 * addition to cover the {@code ProcessBuilder} usage sites in Solr core (FR-007).
 *
 * <p>Known legitimate process-spawning call sites in Solr (kept out of the default production
 * policy because they use {@code ProcessHandle}, not {@code ProcessBuilder}):
 *
 * <ul>
 *   <li>{@code org.apache.solr.cli.SolrProcessManager} — JVM discovery
 * </ul>
 */
public final class ProcessExecInterceptor {

  private ProcessExecInterceptor() {}

  /** Called before {@code ProcessBuilder.start()}. */
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onProcessBuilderStart() {
    checkExec("ProcessBuilder.start()");
  }

  /**
   * Called before {@code Runtime.exec(String[])}.
   *
   * @param command the command array (first element used for the violation log target)
   */
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onRuntimeExec(@Advice.Argument(0) String[] command) {
    String target =
        command != null && command.length > 0
            ? "Runtime.exec(" + command[0] + ")"
            : "Runtime.exec()";
    checkExec(target);
  }

  // ---------------------------------------------------------------------------
  // Core check logic
  // ---------------------------------------------------------------------------

  /**
   * Checks whether the current top caller is in the approved exec-caller list. Delegates to {@link
   * SecurityViolationLogger} on violation.
   *
   * @param target a human-readable description of the intercepted call for the violation log
   */
  static void checkExec(String target) {
    if (!SolrSecurityPolicy.isInitialized()) return;

    SolrSecurityPolicy policy = SolrSecurityPolicy.getInstance();
    String caller = StackInspector.topCallerClassName();

    if (!policy.isExecApproved(caller)) {
      ViolationMetricsReporter.incrementExec();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.PROCESS_EXEC,
          target,
          caller,
          policy.enforcementMode());
      if (policy.enforcementMode() == SolrSecurityPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "Process spawning denied by Solr security agent — unapproved caller: " + caller);
      }
    }
  }
}

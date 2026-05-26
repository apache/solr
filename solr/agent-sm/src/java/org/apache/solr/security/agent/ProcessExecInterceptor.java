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

import java.lang.reflect.Method;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy {@link Advice} interceptor for child process spawning.
 *
 * <p>Intercepts {@code ProcessBuilder.start()} and {@code Runtime.exec()} to enforce the {@link
 * AgentPolicy#approvedExecCallers()} list. By default, no call sites are approved in the production
 * policy (the list is empty), so all process-spawning attempts will be flagged unless an operator
 * explicitly adds an entry to {@code agent-security-extra.policy}.
 *
 * <p>This is a Solr-specific interceptor to cover the {@code ProcessBuilder} usage sites in Solr
 * core (FR-007).
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

  /**
   * Single entry point for both {@code ProcessBuilder.start()} and {@code Runtime.exec()}.
   *
   * <p>ByteBuddy requires exactly one {@code @OnMethodEnter} method per advice class. This method
   * is registered on both {@code ProcessBuilder} and {@code Runtime} via separate {@code
   * AgentBuilder} transform chains in {@link SolrAgentEntryPoint}.
   *
   * @param args all arguments of the intercepted method
   * @param method the intercepted method (used to identify the call site in the violation log)
   */
  @Advice.OnMethodEnter(suppress = java.io.IOException.class)
  public static void onExec(@Advice.AllArguments Object[] args, @Advice.Origin Method method) {
    checkExec(deriveTarget(method.getName(), args));
  }

  public static String deriveTarget(String methodName, Object[] args) {
    if (args == null || args.length == 0) return methodName + "()";
    Object arg0 = args[0];
    if (arg0 instanceof String[]) {
      String[] cmd = (String[]) arg0;
      return methodName + "(" + (cmd.length > 0 ? cmd[0] : "") + ")";
    }
    if (arg0 instanceof String) return methodName + "(" + arg0 + ")";
    return methodName + "()";
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
  public static void checkExec(String target) {
    if (!AgentPolicy.isInitialized()) return;

    AgentPolicy policy = AgentPolicy.getInstance();
    String caller =
        StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
            .getCallerClass()
            .getName();

    if (!policy.isExecApproved(caller)) {
      ViolationMetricsReporter.incrementExec();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.PROCESS_EXEC,
          target,
          caller,
          policy.enforcementMode());
      if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "Process spawning denied by Solr security agent — unapproved caller: " + caller);
      }
    }
  }
}

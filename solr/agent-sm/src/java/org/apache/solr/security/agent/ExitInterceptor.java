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
 * ByteBuddy {@link Advice} interceptor for {@code System.exit()} and {@code Runtime.halt()}.
 *
 * <p>Checks the top caller class against the {@link SolrSecurityPolicy#approvedExitCallers()} list.
 * If the caller is not approved, the violation is logged and (in enforce mode) a {@link
 * SecurityException} is thrown, preventing the JVM from terminating.
 *
 * <p>Default approved callers (from the bundled default policy):
 *
 * <ul>
 *   <li>{@code org.apache.solr.cli.SolrCLI} — CLI shutdown commands
 *   <li>{@code org.apache.solr.servlet.SolrDispatchFilter} — servlet container shutdown hook
 * </ul>
 *
 * <p>Operators may add additional approved callers via {@code agent-security-extra.policy} using a
 * codeBase-scoped {@code RuntimePermission "exitVM"} grant.
 */
public final class ExitInterceptor {

  private ExitInterceptor() {}

  /**
   * Called before {@code System.exit(int)}.
   *
   * @param status the exit status code (unused for the policy check)
   */
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onSystemExit(@Advice.Argument(0) int status) {
    checkExit("System.exit(" + status + ")");
  }

  /**
   * Called before {@code Runtime.halt(int)}.
   *
   * @param status the halt status code (unused for the policy check)
   */
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onRuntimeHalt(@Advice.Argument(0) int status) {
    checkExit("Runtime.halt(" + status + ")");
  }

  // ---------------------------------------------------------------------------
  // Core check logic
  // ---------------------------------------------------------------------------

  /**
   * Checks whether the current top caller is approved to call {@code System.exit()} or {@code
   * Runtime.halt()}. Delegates to {@link SecurityViolationLogger} on violation.
   *
   * @param target a human-readable description of the intercepted call for the violation log
   */
  static void checkExit(String target) {
    if (!SolrSecurityPolicy.isInitialized()) return;

    SolrSecurityPolicy policy = SolrSecurityPolicy.getInstance();
    String caller = StackInspector.topCallerClassName();

    if (!policy.isExitApproved(caller)) {
      ViolationMetricsReporter.incrementExit();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.SYSTEM_EXIT,
          target,
          caller,
          policy.enforcementMode());
      if (policy.enforcementMode() == SolrSecurityPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "System.exit() / Runtime.halt() denied by Solr security agent — unapproved caller: "
                + caller);
      }
    }
  }
}

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
import java.lang.reflect.Method;
import java.util.Collection;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy {@link Advice} interceptor for {@code ProcessBuilder.start()} and {@code
 * Runtime.exec()}. Permission is granted if any class in the full call chain matches an approved
 * entry — same semantics as {@link SystemExitInterceptor}.
 */
public final class ProcessExecInterceptor {

  private ProcessExecInterceptor() {}

  /** Shared entry point for {@code ProcessBuilder.start()} and {@code Runtime.exec()}. */
  @Advice.OnMethodEnter(suppress = IOException.class)
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
   * Checks the call chain against the approved exec-caller list; logs and throws in enforce mode.
   */
  public static void checkExec(String target) {
    if (!AgentPolicy.isInitialized()) return;

    AgentPolicy policy = AgentPolicy.getInstance();
    final StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    final Class<?> caller = walker.getCallerClass();
    final Collection<Class<?>> chain = walker.walk(StackCallerClassChainExtractor.INSTANCE);

    if (!policy.isChainThatCanExec(chain)) {
      ViolationMetricsReporter.incrementExec();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.PROCESS_EXEC,
          target,
          caller.getName(),
          policy.enforcementMode());
      if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "Process spawning denied by Solr security agent — unapproved caller: "
                + caller.getName());
      }
    }
  }
}

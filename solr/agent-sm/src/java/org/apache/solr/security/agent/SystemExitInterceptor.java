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

import java.lang.StackWalker.Option;
import java.util.Collection;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy {@link Advice} interceptor for {@link System#exit(int)}.
 *
 * <p>Uses the full call-chain check: any class in the stack that matches an approved exit-caller
 * pattern in {@link AgentPolicy} grants permission. In {@link AgentPolicy.EnforcementMode#WARN warn
 * mode} a violation is logged but the exit is allowed to proceed; in {@link
 * AgentPolicy.EnforcementMode#ENFORCE enforce mode} a {@link SecurityException} is thrown.
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
public class SystemExitInterceptor {

  private SystemExitInterceptor() {}

  @Advice.OnMethodEnter
  public static void intercept(@Advice.Argument(0) int code) throws Exception {
    if (!AgentPolicy.isInitialized()) return;
    final AgentPolicy policy = AgentPolicy.getInstance();

    final StackWalker walker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);
    final Class<?> caller = walker.getCallerClass();
    final Collection<Class<?>> chain = walker.walk(StackCallerClassChainExtractor.INSTANCE);

    if (!policy.isChainThatCanExit(chain)) {
      ViolationMetricsReporter.incrementExit();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.SYSTEM_EXIT,
          "exit(" + code + ")",
          caller.getName(),
          policy.enforcementMode());
      if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "The class " + caller.getName() + " is not allowed to call System::exit(" + code + ")");
      }
    }
  }
}

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
 * ByteBuddy {@link Advice} interceptor for {@link Runtime#halt(int)}.
 *
 * <p>Identical logic to {@link SystemExitInterceptor}; registered separately because ByteBuddy
 * requires one advice class per instrumented method.
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
public class RuntimeHaltInterceptor {

  private RuntimeHaltInterceptor() {}

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
          "halt(" + code + ")",
          caller.getName(),
          policy.enforcementMode());
      if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "The class "
                + caller.getName()
                + " is not allowed to call Runtime::halt("
                + code
                + ")");
      }
    }
  }
}

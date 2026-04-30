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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Walks the current call stack and returns the ordered list of non-JDK caller classes.
 *
 * <p>This class uses {@link StackWalker#getInstance(StackWalker.Option)} with {@code
 * StackWalker.Option#RETAIN_CLASS_REFERENCE} to obtain live {@link Class} objects for each frame.
 * JDK-internal frames (whose class is loaded from the {@code jrt:/} location) are filtered out.
 *
 * <h2>Virtual thread compatibility</h2>
 *
 * {@code StackWalker} is virtual-thread–safe by specification. Unlike deprecated {@code
 * Thread.currentThread()} or {@code ThreadGroup} approaches, this implementation does not assume
 * thread identity, making enforcement decisions correct for both platform threads and Project Loom
 * virtual threads.
 */
public final class StackInspector {

  /**
   * StackWalker with class references retained. May be {@code null} if the security environment
   * (e.g. Java SecurityManager in tests) denies the {@code getStackWalkerWithClassReference}
   * permission — in that case caller identification returns {@code "<unknown>"}.
   */
  private static final StackWalker WALKER;

  static {
    StackWalker w;
    try {
      w = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    } catch (SecurityException e) {
      // Java SecurityManager denied the RuntimePermission "getStackWalkerWithClassReference".
      // This can happen in constrained test environments. Fall back to null; all caller-based
      // checks will treat the caller as "<unknown>" (no class reference available).
      w = null;
    }
    WALKER = w;
  }

  /**
   * The agent's own interceptor and infrastructure classes. These are always on the call stack when
   * enforcement is triggered and must be excluded from caller identification.
   */
  private static final Set<String> AGENT_INFRASTRUCTURE =
      Set.of(
          "org.apache.solr.security.agent.StackInspector",
          "org.apache.solr.security.agent.FileAccessInterceptor",
          "org.apache.solr.security.agent.NetworkAccessInterceptor",
          "org.apache.solr.security.agent.ExitInterceptor",
          "org.apache.solr.security.agent.ProcessExecInterceptor",
          "org.apache.solr.security.agent.SolrSecurityPolicy",
          "org.apache.solr.security.agent.SecurityViolationLogger",
          "org.apache.solr.security.agent.ViolationMetricsReporter",
          "org.apache.solr.security.agent.PolicyLoader",
          "org.apache.solr.security.agent.SolrAgentEntryPoint");

  private StackInspector() {}

  /**
   * Returns an ordered list of non-JDK caller classes, starting from the immediate caller of the
   * intercepted method and working up the call chain. JDK classes (those loaded from {@code jrt:/}
   * or whose class loader is {@code null} — i.e. the bootstrap loader) and ByteBuddy-generated
   * classes are excluded.
   *
   * @return caller classes in call-chain order (innermost first), never {@code null}
   */
  public static List<Class<?>> callerClasses() {
    if (WALKER == null) return List.of();
    return WALKER.walk(
        frames -> {
          List<Class<?>> callers = new ArrayList<>();
          frames.forEach(
              frame -> {
                Class<?> cls = frame.getDeclaringClass();
                if (!isJdkClass(cls) && !isBytebuddyClass(cls) && !isAgentClass(cls)) {
                  callers.add(cls);
                }
              });
          return callers;
        });
  }

  /**
   * Returns the fully-qualified name of the first non-JDK, non-ByteBuddy class in the call stack,
   * or {@code "<unknown>"} if none is found.
   */
  public static String topCallerClassName() {
    if (WALKER == null) return "<unknown>";
    return WALKER.walk(
        frames ->
            frames
                .filter(
                    f ->
                        !isJdkClass(f.getDeclaringClass())
                            && !isBytebuddyClass(f.getDeclaringClass())
                            && !isAgentClass(f.getDeclaringClass()))
                .findFirst()
                .map(f -> f.getDeclaringClass().getName())
                .orElse("<unknown>"));
  }

  /**
   * Returns {@code true} if the class should be considered a JDK internal frame and excluded from
   * caller analysis. This covers:
   *
   * <ul>
   *   <li>Classes whose classloader is {@code null} (bootstrap loader — Java platform classes)
   *   <li>Classes from the {@code java.*}, {@code javax.*}, {@code sun.*}, {@code jdk.*} packages
   * </ul>
   */
  static boolean isJdkClass(Class<?> cls) {
    if (cls.getClassLoader() == null) return true;
    String name = cls.getName();
    return name.startsWith("java.")
        || name.startsWith("javax.")
        || name.startsWith("sun.")
        || name.startsWith("jdk.")
        || name.startsWith("com.sun.");
  }

  /**
   * Returns {@code true} if this class is one of the agent's own interceptor or infrastructure
   * classes that are always on the call stack when enforcement is triggered and must not be
   * reported as the "caller". This uses an explicit allowlist so that test classes in the same
   * package are not inadvertently excluded.
   */
  static boolean isAgentClass(Class<?> cls) {
    return AGENT_INFRASTRUCTURE.contains(cls.getName());
  }

  /**
   * Returns {@code true} if this class is a ByteBuddy-generated instrumentation proxy that should
   * not appear in violation call-site analysis.
   */
  static boolean isBytebuddyClass(Class<?> cls) {
    String name = cls.getName();
    return name.contains("$ByteBuddy$") || name.startsWith("net.bytebuddy.");
  }
}

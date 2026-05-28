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

import java.util.List;

/**
 * A call-site pattern that is approved to perform a restricted operation such as calling {@code
 * System.exit()} or spawning a child process via {@code ProcessBuilder}.
 *
 * <p>Approved call sites are loaded from the security policy at startup and are immutable. Each
 * entry matches callers either by class-name pattern or by code-source location (codeBase).
 *
 * <p><b>Class-name matching</b> (when {@link #codeBase()} is {@code null}): uses the same syntax as
 * {@link AgentPolicy#isChainThatCanExit}: {@code "*"} matches any class; a pattern ending in {@code
 * ".*"} (e.g. {@code org.apache.solr.cli.*}) matches that package and all sub-packages; anything
 * else is an exact fully-qualified class name.
 *
 * <p><b>codeBase matching</b> (when {@link #codeBase()} is non-{@code null}): the calling class
 * must have been loaded from a JAR or directory matching the codeBase URL, using the same syntax as
 * JDK policy files ({@code file:/path/-} for recursive, {@code file:/path/to.jar} for exact).
 *
 * <p>Default approved EXIT callers: none (empty list in the bundled production policy). Operators
 * can add entries via {@code agent-security-extra.policy} when specific code needs to call {@code
 * System.exit()} — for example, a custom lifecycle plugin.
 *
 * <p>Default approved EXEC callers: none (empty list in production policy).
 */
public final class ApprovedCallSite {

  /** The restricted operation covered by this approval. */
  public enum Operation {
    EXIT,
    EXEC
  }

  private final String classNamePattern; // null when codeBase matching is used
  private final String codeBase; // null when class-name matching is used
  private final Operation operation;
  private final PolicySource source;

  /** Constructs a class-name–based approval (no codeBase constraint). */
  ApprovedCallSite(String classNamePattern, Operation operation, PolicySource source) {
    this(classNamePattern, null, operation, source);
  }

  /**
   * Constructs an approval that is either class-name–based ({@code codeBase == null}) or
   * code-source–based ({@code classNamePattern == null}).
   */
  ApprovedCallSite(
      String classNamePattern, String codeBase, Operation operation, PolicySource source) {
    this.classNamePattern = classNamePattern;
    this.codeBase = codeBase;
    this.operation = operation;
    this.source = source;
  }

  /**
   * Fully-qualified class name or prefix pattern (ending in {@code .*}), or {@code null} when this
   * entry uses codeBase matching instead.
   */
  public String classNamePattern() {
    return classNamePattern;
  }

  /**
   * JDK-style codeBase URL for code-source matching (e.g. {@code file:/opt/solr/modules/foo/-}), or
   * {@code null} when this entry uses class-name matching instead.
   */
  public String codeBase() {
    return codeBase;
  }

  /** The restricted operation this approval covers. */
  public Operation operation() {
    return operation;
  }

  /** Whether this entry came from the default bundled policy or an operator extension. */
  public PolicySource source() {
    return source;
  }

  /**
   * Returns {@code true} if the given fully-qualified class name matches this entry's class-name
   * pattern. Always returns {@code false} when this entry uses codeBase matching — use {@link
   * #matchesCodeBase(Class)} instead.
   */
  public boolean matches(String className) {
    if (codeBase != null) return false; // codeBase entries must be checked via matchesCodeBase
    if ("*".equals(classNamePattern)) return true;
    if (classNamePattern.endsWith(".*")) {
      String prefix = classNamePattern.substring(0, classNamePattern.length() - 2);
      return className.equals(prefix) || className.startsWith(prefix + ".");
    }
    return classNamePattern.equals(className);
  }

  /**
   * Returns {@code true} if this entry has a codeBase constraint and the given class was loaded
   * from a code source matching that codeBase. Always returns {@code false} when this entry uses
   * class-name matching.
   */
  public boolean matchesCodeBase(Class<?> cls) {
    if (codeBase == null) return false;
    return SocketChannelInterceptor.isCallerFromCodeBase(List.of(cls), codeBase);
  }
}

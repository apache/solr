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

/**
 * A class (or class-name prefix pattern) that is approved to perform a restricted operation such as
 * calling {@code System.exit()} or spawning a child process via {@code ProcessBuilder}.
 *
 * <p>Approved call sites are loaded from the security policy at startup and are immutable. Entries
 * use either an exact fully-qualified class name or a prefix ending in {@code .*} (e.g. {@code
 * org.apache.solr.cli.*}).
 *
 * <p>Default approved EXIT callers:
 *
 * <ul>
 *   <li>{@code org.apache.solr.cli.SolrCLI}
 *   <li>{@code org.apache.solr.servlet.SolrDispatchFilter}
 * </ul>
 *
 * <p>Default approved EXEC callers: none (empty list in production policy).
 */
public final class ApprovedCallSite {

  /** The restricted operation covered by this approval. */
  public enum Operation {
    EXIT,
    EXEC
  }

  private final String classNamePattern;
  private final Operation operation;
  private final PolicySource source;

  ApprovedCallSite(String classNamePattern, Operation operation, PolicySource source) {
    this.classNamePattern = classNamePattern;
    this.operation = operation;
    this.source = source;
  }

  /** Fully-qualified class name or prefix pattern (ending in {@code .*}). */
  public String classNamePattern() {
    return classNamePattern;
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
   * Returns {@code true} if the given fully-qualified class name matches this approved call-site
   * pattern.
   */
  public boolean matches(String className) {
    if ("*".equals(classNamePattern)) return true;
    if (classNamePattern.endsWith(".*")) {
      String prefix = classNamePattern.substring(0, classNamePattern.length() - 2);
      return className.equals(prefix) || className.startsWith(prefix + ".");
    }
    return classNamePattern.equals(className);
  }
}

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
 * A policy entry approving a class (or code source) to perform a restricted operation.
 *
 * <p>Class-name matching ({@link #codeBase()} is {@code null}): {@code "*"} matches any class; a
 * pattern ending in {@code ".*"} matches that package and sub-packages; otherwise exact match.
 * codeBase matching: the calling class must have been loaded from a location matching the JDK
 * policy {@code codeBase} URL ({@code file:/path/-} recursive, {@code file:/path/to.jar} exact).
 */
public record ApprovedCallSite(
    String classNamePattern, String codeBase, Operation operation, PolicySource source) {

  /** The restricted operation covered by this approval. */
  public enum Operation {
    EXIT,
    EXEC
  }

  ApprovedCallSite(String classNamePattern, Operation operation, PolicySource source) {
    this(classNamePattern, null, operation, source);
  }

  /**
   * Returns {@code true} if {@code className} matches the pattern; {@code false} for codeBase
   * entries.
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
   * Returns {@code true} if {@code cls} was loaded from this entry's codeBase; {@code false} for
   * class-name entries.
   */
  public boolean matchesCodeBase(Class<?> cls) {
    if (codeBase == null) return false;
    return SocketChannelInterceptor.isCallerFromCodeBase(List.of(cls), codeBase);
  }
}

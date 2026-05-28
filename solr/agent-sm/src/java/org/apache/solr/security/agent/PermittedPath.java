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

import java.nio.file.Path;
import java.util.Locale;

/**
 * A single file-system access rule from the security policy.
 *
 * <p>Rules are loaded at startup and are immutable thereafter. A rule grants access to a path (and
 * optionally all descendants) for the set of operations encoded in the {@code actions} string
 * ({@code "read"}, {@code "write"}, {@code "delete"}, or any comma-separated combination).
 */
public final class PermittedPath {

  private final String path;
  private final String actions;
  private final boolean recursive;
  private final PolicySource source;

  PermittedPath(String path, String actions, boolean recursive, PolicySource source) {
    this.path = path;
    this.actions = actions != null ? actions.toLowerCase(Locale.ROOT) : "read";
    this.recursive = recursive;
    this.source = source;
  }

  /** The base path after variable substitution. */
  public String path() {
    return path;
  }

  /** Comma-separated actions string, lower-cased (e.g. {@code "read,write,delete"}). */
  public String actions() {
    return actions;
  }

  /** Whether the rule covers all descendants ({@code path/-} in policy syntax). */
  public boolean recursive() {
    return recursive;
  }

  /** Whether this rule came from the default bundled policy or an operator extension. */
  public PolicySource source() {
    return source;
  }

  /** Returns {@code true} if this rule permits the given action on the given resolved path. */
  public boolean permits(String resolvedPath, String action) {
    boolean pathMatch;
    if (recursive) {
      // Use Path.startsWith(Path) rather than a string prefix check so that component boundaries
      // are respected and path-separator style differences (e.g. forward vs back slash on Windows)
      // are handled by the platform Path implementation.
      pathMatch = Path.of(resolvedPath).startsWith(Path.of(path));
    } else {
      pathMatch = resolvedPath.equals(path);
    }
    return pathMatch && actions.contains(action.toLowerCase(Locale.ROOT));
  }
}

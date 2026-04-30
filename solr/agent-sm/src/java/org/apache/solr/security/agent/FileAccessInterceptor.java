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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy {@link Advice} interceptor for file-system operations.
 *
 * <p>This class is injected into the bootstrap classloader via {@link
 * net.bytebuddy.dynamic.loading.ClassInjector.UsingUnsafe} so that it can intercept JDK methods.
 * Every public method annotated with {@code Advice.OnMethodEnter} runs before the intercepted
 * method body.
 *
 * <h2>Enforcement</h2>
 *
 * <ol>
 *   <li>Windows UNC paths ({@code \\host\share\...}) are rejected unconditionally regardless of any
 *       policy rule (FR-003).
 *   <li>Symlinks are resolved to their real path via {@code toRealPath()} before the policy check,
 *       preventing symlink-escape attacks (FR-004).
 *   <li>The resolved path is checked against {@link SolrSecurityPolicy#permittedPaths()} for the
 *       relevant action.
 *   <li>Violations are handed to {@link SecurityViolationLogger} and, in enforce mode, result in a
 *       {@link SecurityException} being thrown.
 * </ol>
 *
 * <h2>Trusted filesystems</h2>
 *
 * Paths on filesystem schemes listed in {@link SolrSecurityPolicy#trustedFileSystems()} (e.g.
 * in-memory filesystems used by tests) are exempt from enforcement.
 */
public final class FileAccessInterceptor {

  private FileAccessInterceptor() {}

  /**
   * Called before any NIO file-read operation. Checks the resolved path against the policy.
   *
   * @param path the {@link Path} argument of the intercepted method
   */
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onFileRead(@Advice.Argument(0) Path path) {
    checkPath(path, "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }

  /**
   * Called before any NIO file-write or create operation.
   *
   * @param path the {@link Path} argument of the intercepted method
   */
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onFileWrite(@Advice.Argument(0) Path path) {
    checkPath(path, "write", SecurityViolationLogger.ViolationType.FILE_WRITE);
  }

  /**
   * Called before any NIO file-delete operation.
   *
   * @param path the {@link Path} argument of the intercepted method
   */
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onFileDelete(@Advice.Argument(0) Path path) {
    checkPath(path, "delete", SecurityViolationLogger.ViolationType.FILE_DELETE);
  }

  // ---------------------------------------------------------------------------
  // Core check logic — shared by all three entry points
  // ---------------------------------------------------------------------------

  /**
   * Performs the UNC check, symlink resolution, and policy lookup for a given path and action.
   * Delegates to {@link SecurityViolationLogger} on violation, and (in enforce mode) throws {@link
   * SecurityException}.
   */
  static void checkPath(Path path, String action, SecurityViolationLogger.ViolationType type) {
    if (path == null) return;

    // Check if the policy singleton is available yet; skip if not (very early startup).
    if (!SolrSecurityPolicy.isInitialized()) return;

    SolrSecurityPolicy policy = SolrSecurityPolicy.getInstance();

    // Skip trusted filesystem schemes (e.g. in-memory FS used by tests).
    String scheme = path.toUri().getScheme();
    if (scheme != null && policy.trustedFileSystems().contains(scheme)) return;

    String rawPathStr = path.toAbsolutePath().toString();

    // FR-003: Block Windows UNC paths unconditionally on all platforms.
    if (isUncPath(rawPathStr)) {
      String caller = StackInspector.topCallerClassName();
      ViolationMetricsReporter.incrementFile();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.FILE_READ,
          rawPathStr,
          caller,
          policy.enforcementMode(),
          "UNC_BLOCKED");
      if (policy.enforcementMode() == SolrSecurityPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException("UNC path access denied by Solr security agent: " + rawPathStr);
      }
      return;
    }

    // FR-004: Resolve symlinks before the policy check.
    String resolvedPathStr = resolveRealPath(path, rawPathStr);

    // Check against policy.
    if (!policy.isPathPermitted(resolvedPathStr, action)) {
      String caller = StackInspector.topCallerClassName();
      ViolationMetricsReporter.incrementFile();
      SecurityViolationLogger.log(type, resolvedPathStr, caller, policy.enforcementMode());
      if (policy.enforcementMode() == SolrSecurityPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "File " + action + " denied by Solr security agent: " + resolvedPathStr);
      }
    }
  }

  /** String-based overload for legacy {@link java.io.File} paths. */
  static void checkPath(String pathStr, String action, SecurityViolationLogger.ViolationType type) {
    if (pathStr == null || pathStr.isEmpty()) return;
    checkPath(Path.of(pathStr), action, type);
  }

  /**
   * Returns {@code true} if the given path string is a Windows UNC path ({@code \\host\share\...}
   * or the forward-slash equivalent {@code //host/share/...}). This check is platform-independent
   * so UNC paths are rejected on Linux and macOS too.
   */
  static boolean isUncPath(String pathStr) {
    return pathStr.startsWith("\\\\") || pathStr.startsWith("//");
  }

  /**
   * Resolves the real (symlink-free) path. Returns the original path string if {@code toRealPath()}
   * throws an {@link IOException} (e.g. the file does not yet exist — a pre-create check) or if a
   * {@code SecurityException} is thrown (e.g. by the Java SecurityManager in the test environment).
   * The unresolved path string is still subject to the policy check.
   */
  private static String resolveRealPath(Path path, String fallback) {
    try {
      return path.toRealPath().toString();
    } catch (IOException | SecurityException e) {
      // File may not exist yet (pre-create), or access is restricted; use the normalized path.
      return path.normalize().toAbsolutePath().toString();
    }
  }

  // ---------------------------------------------------------------------------
  // Legacy java.io.File path interception helper
  // ---------------------------------------------------------------------------

  /**
   * Entry point for {@code java.io.File}-based operations (e.g. {@code FileInputStream}, {@code
   * FileOutputStream}). Converts the {@code File} to a {@code Path} and delegates.
   */
  @SuppressForbidden(
      reason =
          "java.io.File is the parameter type of the intercepted legacy JDK method "
              + "(e.g. FileInputStream(File)); the Advice method signature must match.")
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onFileIo(@Advice.Argument(0) File file) {
    if (file == null) return;
    checkPath(file.toPath(), "read", SecurityViolationLogger.ViolationType.FILE_READ);
  }
}

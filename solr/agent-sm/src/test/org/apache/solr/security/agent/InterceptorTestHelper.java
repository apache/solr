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

import java.net.InetSocketAddress;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Test-only helpers that exercise interceptor enforcement logic without ByteBuddy instrumentation.
 * All enforcement is delegated to the same shared methods used by the live advice.
 */
class InterceptorTestHelper {

  private InterceptorTestHelper() {}

  /**
   * Resolves {@code path} to its real path (following symlinks), falling back to {@code
   * normalize().toAbsolutePath()} on I/O error.
   */
  static String resolveRealPath(Path path) {
    try {
      return path.toRealPath(new LinkOption[0]).toString();
    } catch (Exception e) {
      return path.normalize().toAbsolutePath().toString();
    }
  }

  /** Returns the name of the calling class for use as the {@code caller} field in violations. */
  static String callerClassName() {
    try {
      return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
          .getCallerClass()
          .getName();
    } catch (Exception e) {
      return "<unknown>";
    }
  }

  /**
   * Checks whether {@code path} may be accessed with {@code action} under the active policy.
   * Delegates to {@link FileInterceptor#enforceFileAccess}.
   */
  static void checkPath(Path path, String action, SecurityViolationLogger.ViolationType type) {
    if (!AgentPolicy.isInitialized()) return;
    AgentPolicy policy = AgentPolicy.getInstance();
    String resolved = resolveRealPath(path);
    FileInterceptor.enforceFileAccess(
        policy,
        resolved,
        action,
        type,
        callerClassName(),
        "Denied " + action.toUpperCase(Locale.ROOT) + " access to: " + resolved);
  }

  /**
   * Checks whether a move from {@code source} to {@code target} is permitted. Source requires
   * "delete"; target requires "write". Delegates to {@link FileInterceptor#enforceFileAccess}.
   */
  static void checkMove(Path source, Path target) {
    if (!AgentPolicy.isInitialized()) return;
    AgentPolicy policy = AgentPolicy.getInstance();
    String srcPath = resolveRealPath(source);
    String dstPath = resolveRealPath(target);
    String caller = callerClassName();
    FileInterceptor.enforceFileAccess(
        policy,
        srcPath,
        "delete",
        SecurityViolationLogger.ViolationType.FILE_DELETE,
        caller,
        "Denied MOVE (delete source) access to: " + srcPath);
    FileInterceptor.enforceFileAccess(
        policy,
        dstPath,
        "write",
        SecurityViolationLogger.ViolationType.FILE_WRITE,
        caller,
        "Denied MOVE (write destination) access to: " + dstPath);
  }

  /**
   * Checks whether connecting to {@code address} is permitted under the active policy. Delegates to
   * {@link SocketChannelInterceptor#enforceNetworkAccess}.
   */
  static void checkConnect(InetSocketAddress address) {
    if (!AgentPolicy.isInitialized()) return;
    if (address.isUnresolved()) return;
    AgentPolicy policy = AgentPolicy.getInstance();
    if (policy.trustedHosts().contains(address.getHostString())) return;
    String host = address.getHostString();
    int port = address.getPort();
    SocketChannelInterceptor.enforceNetworkAccess(policy, host, port, callerClassName());
  }
}

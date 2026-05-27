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

import java.lang.reflect.Method;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy {@link Advice} interceptor for file-system operations.
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
public class FileInterceptor {

  /** FileInterceptor */
  public FileInterceptor() {}

  /**
   * Intercepts file operations.
   *
   * @param args arguments
   * @param method method
   * @throws Exception exceptions
   */
  @Advice.OnMethodEnter
  public static void intercept(@Advice.AllArguments Object[] args, @Advice.Origin Method method)
      throws Exception {
    if (!AgentPolicy.isInitialized()) return;
    final AgentPolicy policy = AgentPolicy.getInstance();

    FileSystemProvider provider = null;
    String filePath = null;
    if (args.length > 0 && args[0] instanceof String pathStr) {
      filePath = Path.of(pathStr).toAbsolutePath().toString();
    } else if (args.length > 0 && args[0] instanceof Path path) {
      filePath = path.toAbsolutePath().toString();
      provider = path.getFileSystem().provider();
    }

    if (filePath == null) {
      return; // No valid file path found
    }

    if (provider != null && policy.trustedFileSystems().contains(provider.getScheme())) {
      return;
    }

    final StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    final String caller = walker.getCallerClass().getName();

    final String name = method.getName();
    boolean isMutating = name.equals("move") || name.equals("write") || name.startsWith("create");
    final boolean isDelete = isMutating == false ? name.startsWith("delete") : false;

    // This is Windows implementation of UNIX Domain Sockets (close)
    boolean isUnixSocketCaller = false;
    if (isDelete == true) {
      final Collection<Class<?>> chain = walker.walk(StackCallerClassChainExtractor.INSTANCE);
      for (final Class<?> cls : chain) {
        if (cls.getName().equalsIgnoreCase("sun.nio.ch.PipeImpl$Initializer$LoopbackConnector")) {
          isUnixSocketCaller = true;
          break;
        }
      }
    }

    if (isDelete == true && isUnixSocketCaller == true) {
      // Unix domain socket cleanup — local IPC, always allow
      return;
    } else {
      String targetFilePath = null;
      if (isMutating == false && isDelete == false) {
        if (name.equals("newByteChannel") == true || name.equals("open") == true) {
          if (args.length > 1) {
            if (args instanceof OpenOption[] opts) {
              for (final OpenOption opt : opts) {
                if (opt != StandardOpenOption.READ) {
                  isMutating = true;
                  break;
                }
              }
            } else if (args[1] instanceof Set<?> opts) {
              @SuppressWarnings("unchecked")
              final Set<OpenOption> options = (Set<OpenOption>) args[1];
              for (final OpenOption opt : options) {
                if (opt != StandardOpenOption.READ) {
                  isMutating = true;
                  break;
                }
              }
            } else if (args[1] instanceof Object[] opts) {
              for (final Object opt : opts) {
                if (opt != StandardOpenOption.READ) {
                  isMutating = true;
                  break;
                }
              }
            } else {
              throw new SecurityException(
                  "Unsupported argument type: " + args[1].getClass().getName());
            }
          }
        } else if (name.equals("copy") == true) {
          if (args.length > 1 && args[1] instanceof String pathStr) {
            targetFilePath = Path.of(pathStr).toAbsolutePath().toString();
          } else if (args.length > 1 && args[1] instanceof Path path) {
            targetFilePath = path.toAbsolutePath().toString();
          }
        }
      }

      // Handle FileChannel.open() and newByteChannel() — check read/write permissions
      if (method.getName().equals("open") || method.getName().equals("newByteChannel")) {
        final String action = isMutating ? "write" : "read";
        if (!policy.isPathPermitted(filePath, action)) {
          ViolationMetricsReporter.incrementFile();
          SecurityViolationLogger.log(
              isMutating
                  ? SecurityViolationLogger.ViolationType.FILE_WRITE
                  : SecurityViolationLogger.ViolationType.FILE_READ,
              filePath,
              caller,
              policy.enforcementMode());
          if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
            throw new SecurityException(
                "Denied "
                    + (isMutating ? "OPEN (read/write)" : "OPEN (read)")
                    + " access to file: "
                    + filePath);
          }
        }
      }

      // Handle Files.copy() — check source read and target write permissions
      if (method.getName().equals("copy")) {
        if (!policy.isPathPermitted(filePath, "read")) {
          ViolationMetricsReporter.incrementFile();
          SecurityViolationLogger.log(
              SecurityViolationLogger.ViolationType.FILE_READ,
              filePath,
              caller,
              policy.enforcementMode());
          if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
            throw new SecurityException("Denied COPY (read) access to file: " + filePath);
          }
        }
        if (targetFilePath != null && !policy.isPathPermitted(targetFilePath, "write")) {
          ViolationMetricsReporter.incrementFile();
          SecurityViolationLogger.log(
              SecurityViolationLogger.ViolationType.FILE_WRITE,
              targetFilePath,
              caller,
              policy.enforcementMode());
          if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
            throw new SecurityException("Denied COPY (write) access to file: " + targetFilePath);
          }
        }
      }

      // Plain read operations (e.g. Files.read(), Files.readAllBytes())
      if (name.equals("read") && !policy.isPathPermitted(filePath, "read")) {
        ViolationMetricsReporter.incrementFile();
        SecurityViolationLogger.log(
            SecurityViolationLogger.ViolationType.FILE_READ,
            filePath,
            caller,
            policy.enforcementMode());
        if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
          throw new SecurityException("Denied READ access to file: " + filePath);
        }
      }

      // File mutating operations
      if (isMutating && !policy.isPathPermitted(filePath, "write")) {
        ViolationMetricsReporter.incrementFile();
        SecurityViolationLogger.log(
            SecurityViolationLogger.ViolationType.FILE_WRITE,
            filePath,
            caller,
            policy.enforcementMode());
        if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
          throw new SecurityException("Denied WRITE access to file: " + filePath);
        }
      }

      // File deletion operations
      if (isDelete && !policy.isPathPermitted(filePath, "delete")) {
        ViolationMetricsReporter.incrementFile();
        SecurityViolationLogger.log(
            SecurityViolationLogger.ViolationType.FILE_DELETE,
            filePath,
            caller,
            policy.enforcementMode());
        if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
          throw new SecurityException("Denied DELETE access to file: " + filePath);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Static helpers (used by advice and by tests)
  // ---------------------------------------------------------------------------

  public static String topCallerClassName() {
    try {
      return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
          .getCallerClass()
          .getName();
    } catch (Exception e) {
      return "<unknown>";
    }
  }

  /**
   * Resolves the real path of {@code path}, following symlinks. Falls back to {@code
   * normalize().toAbsolutePath()} if the file does not exist or if an I/O error occurs.
   *
   * <p><b>Note:</b> This method must NOT be called from the ByteBuddy {@link #intercept} advice
   * method — {@code toRealPath()} performs file-system I/O which would trigger re-entrant
   * interception and cause infinite recursion. It is safe to use only from the test-side {@link
   * #checkPath} helper where no live instrumentation is active.
   */
  public static String resolveRealPath(Path path) {
    try {
      return path.toRealPath(new LinkOption[0]).toString();
    } catch (Exception e) {
      return path.normalize().toAbsolutePath().toString();
    }
  }

  /**
   * Checks whether {@code path} may be accessed with {@code action} under the active policy.
   * Increments the file violation counter and logs on violation; throws {@link SecurityException}
   * in enforce mode.
   *
   * <p>Used by tests to exercise the file-access check without ByteBuddy instrumentation.
   */
  public static void checkPath(
      Path path, String action, SecurityViolationLogger.ViolationType violationType) {
    if (!AgentPolicy.isInitialized()) return;
    AgentPolicy policy = AgentPolicy.getInstance();
    String resolvedPath = resolveRealPath(path);
    String caller = topCallerClassName();
    if (!policy.isPathPermitted(resolvedPath, action)) {
      ViolationMetricsReporter.incrementFile();
      SecurityViolationLogger.log(violationType, resolvedPath, caller, policy.enforcementMode());
      if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "Denied " + action.toUpperCase(Locale.ROOT) + " access to: " + resolvedPath);
      }
    }
  }
}

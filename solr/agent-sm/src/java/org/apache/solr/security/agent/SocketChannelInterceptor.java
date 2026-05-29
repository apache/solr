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
import java.net.URL;
import java.net.UnixDomainSocketAddress;
import java.security.CodeSource;
import java.util.Collection;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy {@link Advice} interceptor for outbound socket connections.
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
public class SocketChannelInterceptor {

  public SocketChannelInterceptor() {}

  @Advice.OnMethodEnter
  public static void intercept(@Advice.AllArguments Object[] args) throws Exception {
    if (!AgentPolicy.isInitialized()) return;
    final AgentPolicy policy = AgentPolicy.getInstance();

    final StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    final String caller = walker.getCallerClass().getName();

    if (args[0] instanceof InetSocketAddress address) {
      if (!policy.trustedHosts().contains(address.getHostString())) {
        enforceNetworkAccess(policy, address.getHostString(), address.getPort(), caller);
      }
    } else if (args[0] instanceof UnixDomainSocketAddress) {
      // Unix domain socket — local IPC, always allow
    } else if (args[0] != null) {
      // Unknown SocketAddress subclass — fail closed (host/port unknown, cannot consult policy)
      final String target = args[0].toString();
      ViolationMetricsReporter.incrementNetwork();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.NETWORK_CONNECT,
          target,
          caller,
          policy.enforcementMode());
      if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "Outbound network connection denied (unknown address type): " + target);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Shared enforcement helper
  // ---------------------------------------------------------------------------

  /** Checks the policy for {@code host:port}; logs and throws {@link SecurityException} in enforce mode. */
  static void enforceNetworkAccess(AgentPolicy policy, String host, int port, String caller) {
    if (!isEndpointPermitted(policy, host, port)) {
      String target = host + ":" + port;
      ViolationMetricsReporter.incrementNetwork();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.NETWORK_CONNECT,
          target,
          caller,
          policy.enforcementMode());
      if (policy.enforcementMode() == AgentPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "Outbound network connection denied by Solr security agent: " + target);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Endpoint matching helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} if the policy permits an outbound connection to {@code host:port}.
   * Matching rules: {@code *:port} — any host on that port; {@code host:low-high} — port range;
   * {@code *} — everything. codeBase-restricted entries are checked lazily via {@link StackWalker}.
   */
  public static boolean isEndpointPermitted(AgentPolicy policy, String host, int port) {
    Collection<Class<?>> chain = null; // lazily populated

    for (PermittedEndpoint entry : policy.permittedEndpoints()) {
      if (!matchesEndpoint(entry.hostPort(), host, port)) continue;

      if (entry.codeBase() == null) {
        // Global grant — no code-source restriction
        return true;
      }

      // codeBase-scoped grant: check if any class in the call chain was loaded from that codeBase
      if (chain == null) {
        chain =
            StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                .walk(StackCallerClassChainExtractor.INSTANCE);
      }
      if (isCallerFromCodeBase(chain, entry.codeBase())) return true;
    }
    return false;
  }

  /**
   * Returns {@code true} if any class in {@code chain} has a code source whose location is under
   * the given {@code codeBase} path. Supports JDK policy file {@code codeBase} syntax:
   *
   * <ul>
   *   <li>{@code file:/path/to/dir/-} — recursive: matches any JAR or class in that directory tree
   *   <li>{@code file:/path/to/dir/} or {@code file:/path/to/dir} — exact directory
   *   <li>{@code file:/path/to/specific.jar} — exact JAR file
   * </ul>
   */
  static boolean isCallerFromCodeBase(Collection<Class<?>> chain, String codeBase) {
    // Strip "file:" scheme prefix if present
    String base = codeBase.startsWith("file:") ? codeBase.substring(5) : codeBase;
    boolean recursive = base.endsWith("/-");
    if (recursive) base = base.substring(0, base.length() - 2);
    // Normalise: strip trailing "/" so startsWith checks are consistent
    while (base.endsWith("/") || base.endsWith("\\")) base = base.substring(0, base.length() - 1);

    for (Class<?> cls : chain) {
      try {
        CodeSource cs = cls.getProtectionDomain().getCodeSource();
        if (cs == null) continue;
        URL loc = cs.getLocation();
        if (loc == null) continue;
        String locPath = loc.getPath();
        if (locPath == null) continue;
        // Normalise: strip trailing separators
        while (locPath.endsWith("/") || locPath.endsWith("\\"))
          locPath = locPath.substring(0, locPath.length() - 1);

        // Normalise path separators for cross-platform comparison (Windows may use backslashes)
        String normBase = base.replace('\\', '/');
        String normLocPath = locPath.replace('\\', '/');
        if (recursive) {
          if (normLocPath.equals(normBase) || normLocPath.startsWith(normBase + "/")) return true;
        } else {
          if (normLocPath.equals(normBase)) return true;
        }
      } catch (Exception ignored) {
        // SecurityException or other runtime exception — skip this frame
      }
    }
    return false;
  }

  public static boolean matchesEndpoint(String hostPortEntry, String host, int port) {
    if ("*".equals(hostPortEntry)) return true;

    String entryHost;
    String entryPort;
    if (hostPortEntry.startsWith("[")) {
      // IPv6 bracket notation: "[::1]:8983" or "[::1]:1-65535"
      int closeBracket = hostPortEntry.indexOf(']');
      if (closeBracket < 0) return false;
      entryHost = hostPortEntry.substring(1, closeBracket); // strip brackets
      int colonAfterBracket = hostPortEntry.indexOf(':', closeBracket + 1);
      entryPort = colonAfterBracket >= 0 ? hostPortEntry.substring(colonAfterBracket + 1) : null;
    } else {
      int colonIdx = hostPortEntry.lastIndexOf(':');
      if (colonIdx < 0) {
        return matchesHost(hostPortEntry, host);
      }
      entryHost = hostPortEntry.substring(0, colonIdx);
      entryPort = hostPortEntry.substring(colonIdx + 1);
    }

    if (!matchesHost(entryHost, host)) return false;
    return entryPort == null || matchesPort(entryPort, port);
  }

  public static boolean matchesHost(String entryHost, String actualHost) {
    if ("*".equals(entryHost)) return true;
    return entryHost.equalsIgnoreCase(actualHost);
  }

  public static boolean matchesPort(String entryPort, int actualPort) {
    if ("*".equals(entryPort)) return true;
    int dashIdx = entryPort.indexOf('-');
    if (dashIdx < 0) {
      try {
        return Integer.parseInt(entryPort.trim()) == actualPort;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    try {
      int low = Integer.parseInt(entryPort.substring(0, dashIdx).trim());
      int high = Integer.parseInt(entryPort.substring(dashIdx + 1).trim());
      return actualPort >= low && actualPort <= high;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}

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
import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.Origin;

/**
 * ByteBuddy {@link Advice} interceptor for outbound socket connections.
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
public class SocketChannelInterceptor {

  /** SocketChannelInterceptor */
  public SocketChannelInterceptor() {}

  /**
   * Interceptors
   *
   * @param args arguments
   * @param method method
   * @throws Exception exceptions
   */
  @Advice.OnMethodEnter
  public static void intercept(@Advice.AllArguments Object[] args, @Origin Method method)
      throws Exception {
    if (!AgentPolicy.isInitialized()) return;
    final AgentPolicy policy = AgentPolicy.getInstance();

    final StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    final String caller = walker.getCallerClass().getName();

    if (args[0] instanceof InetSocketAddress address) {
      if (!policy.trustedHosts().contains(address.getHostString())) {
        final String host = address.getHostString();
        final int port = address.getPort();

        if (!isEndpointPermitted(policy, host, port)) {
          final String target = host + ":" + port;
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
    } else if (args[0] instanceof UnixDomainSocketAddress) {
      // Unix domain socket — local IPC, always allow
      return;
    }
  }

  // ---------------------------------------------------------------------------
  // Static helpers (used by advice and by tests)
  // ---------------------------------------------------------------------------

  /**
   * Checks whether the given remote address may be connected to under the active policy. Increments
   * the network violation counter and logs on violation; throws {@link SecurityException} in
   * enforce mode.
   *
   * <p>Used by tests to exercise the network check without ByteBuddy instrumentation.
   */
  public static void checkConnect(java.net.InetSocketAddress address) {
    if (!AgentPolicy.isInitialized()) return;
    if (address.isUnresolved()) return;
    AgentPolicy policy = AgentPolicy.getInstance();
    if (policy.trustedHosts().contains(address.getHostString())) return;
    String caller = topCallerClassName();
    String host = address.getHostString();
    int port = address.getPort();
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

  public static String topCallerClassName() {
    try {
      return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
          .getCallerClass()
          .getName();
    } catch (Exception e) {
      return "<unknown>";
    }
  }

  // ---------------------------------------------------------------------------
  // Endpoint matching helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} if at least one permitted endpoint entry in the policy covers the given
   * host and port. Matching rules:
   *
   * <ul>
   *   <li>Entry {@code *:port} — matches any host on that exact port
   *   <li>Entry {@code host:port} — matches exact host and port
   *   <li>Entry {@code host:low-high} — matches the host with a port in the inclusive range
   *   <li>Entry {@code *} (no colon) — matches everything (broad wildcard)
   * </ul>
   */
  public static boolean isEndpointPermitted(AgentPolicy policy, String host, int port) {
    for (PermittedEndpoint entry : policy.permittedEndpoints()) {
      if (entry.codeBase() != null) continue;
      if (matchesEndpoint(entry.hostPort(), host, port)) return true;
    }
    return false;
  }

  public static boolean matchesEndpoint(String hostPortEntry, String host, int port) {
    if ("*".equals(hostPortEntry)) return true;

    int colonIdx = hostPortEntry.lastIndexOf(':');
    if (colonIdx < 0) {
      return matchesHost(hostPortEntry, host);
    }

    String entryHost = hostPortEntry.substring(0, colonIdx);
    String entryPort = hostPortEntry.substring(colonIdx + 1);

    if (!matchesHost(entryHost, host)) return false;
    return matchesPort(entryPort, port);
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

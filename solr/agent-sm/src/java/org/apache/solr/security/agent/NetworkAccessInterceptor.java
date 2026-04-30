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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy {@link Advice} interceptor for outbound network connections.
 *
 * <p>Intercepts {@code SocketChannel.connect(SocketAddress)} and {@code
 * Socket.connect(SocketAddress)} to enforce the {@link SolrSecurityPolicy#permittedEndpoints()}
 * list. Loopback addresses are unconditionally allowed by this interceptor regardless of policy
 * entries, as a safety net against policy misconfiguration.
 *
 * <p>Port-wildcard entries in the default policy (e.g. {@code *:8983}) are matched by comparing
 * only the port number when the host portion of the entry is {@code *}.
 */
public final class NetworkAccessInterceptor {

  private NetworkAccessInterceptor() {}

  /**
   * Called before {@code SocketChannel.connect()} or {@code Socket.connect()}.
   *
   * @param address the remote address to connect to
   */
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void onConnect(@Advice.Argument(0) SocketAddress address) {
    if (!(address instanceof InetSocketAddress)) return;
    checkConnect((InetSocketAddress) address);
  }

  // ---------------------------------------------------------------------------
  // Core check logic
  // ---------------------------------------------------------------------------

  /**
   * Checks the given resolved remote address against the active policy's permitted endpoint list.
   * Loopback and unresolved addresses are allowed unconditionally.
   */
  static void checkConnect(InetSocketAddress address) {
    if (!SolrSecurityPolicy.isInitialized()) return;
    if (address.isUnresolved()) return;

    InetAddress inetAddress = address.getAddress();
    if (inetAddress != null && inetAddress.isLoopbackAddress()) return;

    SolrSecurityPolicy policy = SolrSecurityPolicy.getInstance();
    int port = address.getPort();
    String host = inetAddress != null ? inetAddress.getHostAddress() : address.getHostName();

    if (!isEndpointPermitted(policy, host, port)) {
      String target = host + ":" + port;
      String caller = StackInspector.topCallerClassName();
      ViolationMetricsReporter.incrementNetwork();
      SecurityViolationLogger.log(
          SecurityViolationLogger.ViolationType.NETWORK_CONNECT,
          target,
          caller,
          policy.enforcementMode());
      if (policy.enforcementMode() == SolrSecurityPolicy.EnforcementMode.ENFORCE) {
        throw new SecurityException(
            "Outbound network connection denied by Solr security agent: " + target);
      }
    }
  }

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
   *
   * <p>CodBase-scoped entries ({@link PermittedEndpoint#codeBase()} non-null) are skipped in this
   * path-based check; they are handled by the JVM's own permission system for code loaded from the
   * specified location.
   */
  static boolean isEndpointPermitted(SolrSecurityPolicy policy, String host, int port) {
    for (PermittedEndpoint entry : policy.permittedEndpoints()) {
      // Skip codeBase-scoped entries; they are handled separately at the module level.
      if (entry.codeBase() != null) continue;

      String hostPort = entry.hostPort();
      if (matchesEndpoint(hostPort, host, port)) return true;
    }
    return false;
  }

  private static boolean matchesEndpoint(String hostPortEntry, String host, int port) {
    if ("*".equals(hostPortEntry)) return true; // broad wildcard

    int colonIdx = hostPortEntry.lastIndexOf(':');
    if (colonIdx < 0) {
      // host-only entry — matches any port on that host
      return matchesHost(hostPortEntry, host);
    }

    String entryHost = hostPortEntry.substring(0, colonIdx);
    String entryPort = hostPortEntry.substring(colonIdx + 1);

    if (!matchesHost(entryHost, host)) return false;

    return matchesPort(entryPort, port);
  }

  private static boolean matchesHost(String entryHost, String actualHost) {
    if ("*".equals(entryHost)) return true;
    return entryHost.equalsIgnoreCase(actualHost);
  }

  private static boolean matchesPort(String entryPort, int actualPort) {
    if ("*".equals(entryPort)) return true;
    int dashIdx = entryPort.indexOf('-');
    if (dashIdx < 0) {
      try {
        return Integer.parseInt(entryPort.trim()) == actualPort;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    // Range: low-high
    try {
      int low = Integer.parseInt(entryPort.substring(0, dashIdx).trim());
      int high = Integer.parseInt(entryPort.substring(dashIdx + 1).trim());
      return actualPort >= low && actualPort <= high;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}

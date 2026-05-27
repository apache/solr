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
 * A single outbound network access rule from the security policy.
 *
 * <p>Entries are loaded at startup and are immutable thereafter. An entry permits outbound
 * connections to a host-and-port pair. The {@code hostPort} string follows JDK {@code
 * SocketPermission} syntax: {@code "host:port"}, {@code "*:8983"} (wildcard host), {@code
 * "host:1-65535"} (port range), etc.
 *
 * <p>An optional {@code codeBase} restricts the grant to code loaded from a specific JAR location.
 * A {@code null} codeBase means the grant applies to all code.
 */
public final class PermittedEndpoint {

  private final String hostPort;
  private final String actions;
  private final String codeBase; // null → applies to all code
  private final PolicySource source;

  PermittedEndpoint(String hostPort, String actions, String codeBase, PolicySource source) {
    this.hostPort = hostPort;
    this.actions = actions != null ? actions : "connect,resolve";
    this.codeBase = codeBase;
    this.source = source;
  }

  /**
   * Host-and-port string as written in the policy file (after variable substitution). Examples:
   * {@code "localhost:8983"}, {@code "*:8983"}, {@code "127.0.0.1:1-65535"}.
   */
  public String hostPort() {
    return hostPort;
  }

  /** Actions string, e.g. {@code "connect,resolve"}. */
  public String actions() {
    return actions;
  }

  /**
   * The {@code codeBase} URL pattern this entry is scoped to, or {@code null} for a global grant.
   * Used for pre-permitting bundled modules (e.g. jwt-auth) while keeping the grant out of reach of
   * arbitrary code.
   */
  public String codeBase() {
    return codeBase;
  }

  /** Whether this rule came from the default bundled policy or an operator extension. */
  public PolicySource source() {
    return source;
  }
}

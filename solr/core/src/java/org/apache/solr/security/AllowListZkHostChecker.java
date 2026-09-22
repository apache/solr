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

package org.apache.solr.security;

import java.util.Collection;
import java.util.Set;
import org.apache.solr.core.NodeConfig;

/**
 * Validates a caller-supplied ZooKeeper connection string against the local cluster's ensemble and
 * a configured allow-list. The ZooKeeper counterpart of {@link AllowListUrlChecker}.
 *
 * <p>A {@code zkHost} value is allowed if any of the following are true:
 *
 * <ul>
 *   <li>It is {@code null} (caller did not specify one; Solr will use the local ensemble).
 *   <li>It equals the local SolrCloud ZooKeeper connection string verbatim.
 *   <li>It appears verbatim in the {@code allowZkHosts} list configured in {@code solr.xml}.
 * </ul>
 *
 * <p>Comparison is strict character-for-character; no normalization of trailing slashes, chroot
 * suffixes, host order, whitespace, or case. Operators must list each form they intend to accept.
 */
public class AllowListZkHostChecker {

  /** {@link org.apache.solr.core.SolrXmlConfig} property to configure the allowed ZK hosts. */
  public static final String ZK_HOST_ALLOW_LIST = "allowZkHosts";

  private final Set<String> allowedZkHosts;
  private final String localZkHost;

  /**
   * @param allowedZkHosts the verbatim ZooKeeper connection strings configured under {@link
   *     #ZK_HOST_ALLOW_LIST} in {@code solr.xml}. May be null or empty.
   * @param localZkHost the local SolrCloud ZooKeeper connection string. May be null.
   */
  public AllowListZkHostChecker(Collection<String> allowedZkHosts, String localZkHost) {
    this.allowedZkHosts =
        allowedZkHosts == null || allowedZkHosts.isEmpty() ? Set.of() : Set.copyOf(allowedZkHosts);
    this.localZkHost = localZkHost;
  }

  /** Creates a checker from the {@link NodeConfig} and the local ZK address. */
  public static AllowListZkHostChecker create(NodeConfig config, String localZkHost) {
    return new AllowListZkHostChecker(config.getAllowZkHosts(), localZkHost);
  }

  /**
   * Returns true if {@code zkHost} is null, equals the local ensemble, or appears verbatim in the
   * configured allow-list.
   */
  public boolean isAllowed(String zkHost) {
    if (zkHost == null) {
      return true;
    }
    if (zkHost.equals(localZkHost)) {
      return true;
    }
    return allowedZkHosts.contains(zkHost);
  }
}

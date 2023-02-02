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

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.core.NodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Validates URLs based on an allow list or a {@link ClusterState} in SolrCloud. */
public class AllowListUrlChecker {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** {@link org.apache.solr.core.SolrXmlConfig} property to configure the allowed URLs. */
  public static final String URL_ALLOW_LIST = "allowUrls";

  /** System property to disable URL checking and {@link #ALLOW_ALL} instead. */
  public static final String DISABLE_URL_ALLOW_LIST = "solr.disable." + URL_ALLOW_LIST;

  /** Clue given in URL-forbidden exceptions messages. */
  public static final String SET_SOLR_DISABLE_URL_ALLOW_LIST_CLUE =
      "Set -D" + DISABLE_URL_ALLOW_LIST + "=true to disable URL allow-list checks.";

  /** Singleton checker which allows all URLs. {@link #isEnabled()} returns false. */
  public static final AllowListUrlChecker ALLOW_ALL;

  static {
    try {
      ALLOW_ALL =
          new AllowListUrlChecker(Collections.emptyList()) {
            @Override
            public void checkAllowList(List<String> urls, ClusterState clusterState) {
              // Allow.
            }

            @Override
            public boolean isEnabled() {
              return false;
            }

            @Override
            public String toString() {
              return getClass().getSimpleName() + " [allow all]";
            }
          };
    } catch (MalformedURLException e) {
      // Never thrown.
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Regex pattern to match any protocol, e.g. http:// https:// s3://. After a match, regex group 1
   * contains the protocol and group 2 the rest.
   */
  private static final Pattern PROTOCOL_PATTERN = Pattern.compile("(\\w+)(://.*)");

  /** Allow list of hosts. Elements in the list will be host:port (no protocol or context). */
  private final Set<String> hostAllowList;

  /**
   * @param urlAllowList List of allowed URLs. URLs must be well-formed, missing protocol is
   *     tolerated. An empty list means there is no explicit allow-list of URLs, in this case no URL
   *     is allowed unless a {@link ClusterState} is provided in {@link #checkAllowList(List,
   *     ClusterState)}.
   * @throws MalformedURLException If an URL is invalid.
   */
  public AllowListUrlChecker(List<String> urlAllowList) throws MalformedURLException {
    hostAllowList = parseHostPorts(urlAllowList);
  }

  /**
   * Creates a URL checker based on the {@link NodeConfig} property to configure the allowed URLs.
   */
  public static AllowListUrlChecker create(NodeConfig config) {
    if (Boolean.getBoolean(DISABLE_URL_ALLOW_LIST)) {
      return AllowListUrlChecker.ALLOW_ALL;
    } else if (System.getProperty("solr.disable.shardsWhitelist") != null) {
      log.warn(
          "Property 'solr.disable.shardsWhitelist' is deprecated, please use '{}' instead.",
          DISABLE_URL_ALLOW_LIST);
    }
    try {
      return new AllowListUrlChecker(config.getAllowUrls());
    } catch (MalformedURLException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Invalid URL syntax in '" + URL_ALLOW_LIST + "' configuration: " + config.getAllowUrls(),
          e);
    }
  }

  /**
   * @see #checkAllowList(List, ClusterState)
   */
  public void checkAllowList(List<String> urls) throws MalformedURLException {
    checkAllowList(urls, null);
  }

  /**
   * Checks that the given URLs are present in the configured allow-list or in the provided {@link
   * ClusterState} (in case of cloud mode).
   *
   * @param urls The list of urls to check.
   * @param clusterState The up to date {@link ClusterState}, can be null in case of non-cloud mode.
   * @throws MalformedURLException If an URL is invalid.
   * @throws SolrException If an URL is not present in the allow-list or in the provided {@link
   *     ClusterState}.
   */
  public void checkAllowList(List<String> urls, ClusterState clusterState)
      throws MalformedURLException {
    Set<String> clusterHostAllowList =
        clusterState == null ? Collections.emptySet() : clusterState.getHostAllowList();
    for (String url : urls) {
      String hostPort = parseHostPort(url);
      if (clusterHostAllowList.stream().noneMatch(hostPort::equalsIgnoreCase)
          && hostAllowList.stream().noneMatch(hostPort::equalsIgnoreCase)) {
        throw new SolrException(
            SolrException.ErrorCode.FORBIDDEN,
            "URL "
                + url
                + " is neither a live node of the cluster nor in the configured '"
                + URL_ALLOW_LIST
                + "' "
                + hostAllowList);
      }
    }
  }

  /** Whether this checker has been created with a non-empty allow-list of URLs. */
  public boolean hasExplicitAllowList() {
    return !hostAllowList.isEmpty();
  }

  /** Whether the URL checking is enabled. Only {@link #ALLOW_ALL} returns false. */
  public boolean isEnabled() {
    return true;
  }

  /** Only for testing. */
  @VisibleForTesting
  public Set<String> getHostAllowList() {
    return hostAllowList == null ? null : Collections.unmodifiableSet(hostAllowList);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [allowList=" + hostAllowList + "]";
  }

  @VisibleForTesting
  static Set<String> parseHostPorts(List<String> urls) throws MalformedURLException {
    if (urls == null || urls.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> hostPorts = new HashSet<>();
    for (String urlString : urls) {
      hostPorts.add(parseHostPort(urlString));
    }
    return hostPorts;
  }

  private static String parseHostPort(String url) throws MalformedURLException {
    // Parse the host and port.
    // It doesn't really matter which protocol we set here because we are not going to use it.
    url = url.trim();
    URL u;
    Matcher protocolMatcher = PROTOCOL_PATTERN.matcher(url);
    if (protocolMatcher.matches()) {
      // Replace any protocol unsupported by URL.
      if (!protocolMatcher.group(1).startsWith("http")) {
        url = "http" + protocolMatcher.group(2);
      }
      u = new URL(url);
    } else {
      u = new URL("http://" + url);
    }
    if (u.getHost() == null || u.getPort() < 0) {
      throw new MalformedURLException("Invalid host or port in '" + url + "'");
    }
    return u.getHost() + ":" + u.getPort();
  }
}

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
package org.apache.solr.common.util;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final Pattern URL_PREFIX = Pattern.compile("^([a-z]*?://).*");

  public static String removeScheme(String url) {
    Matcher matcher = URL_PREFIX.matcher(url);
    if (matcher.matches()) {
      return url.substring(matcher.group(1).length());
    }

    return url;
  }

  public static boolean hasScheme(String url) {
    Matcher matcher = URL_PREFIX.matcher(url);
    return matcher.matches();
  }

  public static String getScheme(String url) {
    Matcher matcher = URL_PREFIX.matcher(url);
    if (matcher.matches()) {
      return matcher.group(1);
    }

    return null;
  }

  public static boolean isBaseUrl(String url) {
    final var normalizedUrl = removeTrailingSlashIfPresent(url);
    return normalizedUrl.endsWith("/solr");
  }

  /**
   * @param coreUrl a URL pointing to a specific "core" or collection (i.e. that adheres loosely to
   *     the form "scheme://host:port/solr/coreName")
   * @return a URL pointing to the Solr node's root path
   */
  public static String extractBaseUrl(String coreUrl) {
    coreUrl = removeTrailingSlashIfPresent(coreUrl);

    // Remove the core name and return
    final var indexOfLastSlash = coreUrl.lastIndexOf("/");
    if (indexOfLastSlash == -1) {
      log.warn(
          "Solr core URL [{}] did not contain expected path segments when parsing, ignoring...",
          coreUrl);
      return coreUrl;
    }
    return coreUrl.substring(0, coreUrl.lastIndexOf("/"));
  }

  public static String extractCoreFromCoreUrl(String coreUrl) {
    coreUrl = removeTrailingSlashIfPresent(coreUrl);

    return coreUrl.substring(coreUrl.lastIndexOf("/") + 1);
  }

  /**
   * Create a core URL (e.g. "http://localhost:8983/solr/myCore") from its individual components
   *
   * @param baseUrl a Solr "base URL" (e.g. "http://localhost:8983/solr/")
   * @param coreName the name of a Solr core or collection (with no leading or trailing slashes)
   */
  public static String buildCoreUrl(String baseUrl, String coreName) {
    baseUrl = removeTrailingSlashIfPresent(baseUrl);
    return baseUrl + "/" + coreName;
  }

  private static String removeTrailingSlashIfPresent(String url) {
    if (url.endsWith("/")) {
      return url.substring(0, url.length() - 1);
    }

    return url;
  }

  /**
   * Construct a V1 base url for the Solr node, given its name (e.g., 'app-node-1:8983_solr') and a
   * URL scheme.
   *
   * @param nodeName name of the Solr node
   * @param urlScheme scheme for the base url ('http' or 'https')
   * @return url that looks like {@code https://app-node-1:8983/solr}
   * @throws IllegalArgumentException if the provided node name is malformed
   */
  public static String getBaseUrlForNodeName(final String nodeName, final String urlScheme) {
    return getBaseUrlForNodeName(nodeName, urlScheme, false);
  }

  /**
   * Construct a V1 or a V2 base url for the Solr node, given its name (e.g.,
   * 'app-node-1:8983_solr') and a URL scheme.
   *
   * @param nodeName name of the Solr node
   * @param urlScheme scheme for the base url ('http' or 'https')
   * @param isV2 whether a V2 url should be constructed
   * @return url that looks like {@code https://app-node-1:8983/api} (V2) or {@code
   *     https://app-node-1:8983/solr} (V1)
   * @throws IllegalArgumentException if the provided node name is malformed
   */
  public static String getBaseUrlForNodeName(
      final String nodeName, final String urlScheme, boolean isV2) {
    final int colonAt = nodeName.indexOf(':');
    if (colonAt == -1) {
      throw new IllegalArgumentException(
          "nodeName does not contain expected ':' separator: " + nodeName);
    }

    final int _offset = nodeName.indexOf('_', colonAt);
    if (_offset < 0) {
      throw new IllegalArgumentException(
          "nodeName does not contain expected '_' separator: " + nodeName);
    }
    final String hostAndPort = nodeName.substring(0, _offset);
    return urlScheme + "://" + hostAndPort + "/" + (isV2 ? "api" : "solr");
  }

  /**
   * Construct base Solr URL to a Solr node name
   *
   * @param solrUrl Given a base Solr URL string (e.g., 'https://app-node-1:8983/solr')
   * @return Node name that looks like {@code app-node-1:8983_solr}
   * @throws MalformedURLException if the provided URL string is malformed
   * @throws URISyntaxException if the provided URL string could not be parsed as a URI reference.
   */
  public static String getNodeNameForBaseUrl(String solrUrl)
      throws MalformedURLException, URISyntaxException {
    URL url = new URI(solrUrl).toURL();
    return url.getAuthority() + url.getPath().replace('/', '_');
  }
}

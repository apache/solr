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
          "Solr core URL [{}] did not contain expected path segments when parsing, ignoring...", coreUrl);
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
}

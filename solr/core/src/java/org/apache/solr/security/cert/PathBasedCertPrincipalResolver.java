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
package org.apache.solr.security.cert;

import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a {@link CertPrincipalResolver} that resolves a {@link Principal} from an X509
 * certificate based on configurable paths, filters, and optional extraction patterns. This resolver
 * can extract principal information from various certificate fields, such as Subject DN or SAN
 * fields, according to the specified path. Additionally, it can further refine the extracted value
 * based on optional "after" and "before" patterns, allowing for more precise control over the
 * principal value.
 *
 * <p>Example configuration without extraction pattern:
 *
 * <pre>{@code
 * "principalResolver": {
 *   "class":"solr.PathBasedCertPrincipalResolver",
 *   "params": {
 *     "path":"SAN.email",
 *     "filter":{
 *       "checkType":"startsWith",
 *       "values":["user@example"]
 *     }
 *   }
 * }
 * }</pre>
 *
 * In this configuration, the resolver is directed to extract email addresses from the SAN (Subject
 * Alternative Name) field of the certificate and use them as principal names if they match the
 * specified filter criteria.
 *
 * <p>Example configuration with extraction pattern:
 *
 * <pre>{@code
 * "principalResolver": {
 *   "class":"solr.PathBasedCertPrincipalResolver",
 *   "params": {
 *     "path":"SAN.email",
 *     "filter":{
 *       "checkType":"startsWith",
 *       "values":["email_user1@example"]
 *     },
 *     "extract": {
 *       "after":"_",
 *       "before":"@"
 *     }
 *   }
 * }
 * }</pre>
 *
 * In this extended configuration, after extracting email addresses that match the filter criteria,
 * the resolver further processes the extracted value to include only the portion after the "_"
 * symbol and before "@". This allows for extracting specific parts of the principal value,
 * providing additional flexibility and control.
 */
public class PathBasedCertPrincipalResolver extends PathBasedCertResolverBase
    implements CertPrincipalResolver {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PARAM_EXTRACT = "extract";
  private static final String PARAM_AFTER = "after";
  private static final String PARAM_BEFORE = "before";

  private final CertResolverPattern pattern;
  private final String startPattern;
  private final String endPattern;

  /**
   * Constructs a new PathBasedCertPrincipalResolver with the specified configuration parameters.
   *
   * @param params The configuration parameters specifying the path and filter for extracting
   *     principal information from certificates.
   */
  @SuppressWarnings("unchecked")
  public PathBasedCertPrincipalResolver(Map<String, Object> params) {
    this.pattern = createCertResolverPattern(params, CertUtil.SUBJECT_DN_PREFIX);
    Map<String, String> extractConfig =
        (Map<String, String>) params.getOrDefault(PARAM_EXTRACT, Collections.emptyMap());
    this.startPattern = extractConfig.getOrDefault(PARAM_AFTER, "");
    this.endPattern = extractConfig.getOrDefault(PARAM_BEFORE, "");
  }

  /**
   * Resolves the principal from the given X509 certificate based on the configured path and filter.
   * The first matching value, if any, is used as the principal name.
   *
   * @param certificate The X509Certificate from which to resolve the principal.
   * @return A {@link Principal} object representing the resolved principal from the certificate.
   * @throws SSLPeerUnverifiedException If the SSL peer is not verified.
   * @throws CertificateParsingException If parsing the certificate fails.
   */
  @Override
  public Principal resolvePrincipal(X509Certificate certificate)
      throws SSLPeerUnverifiedException, CertificateParsingException {
    Map<String, Set<String>> matches =
        getValuesFromPaths(certificate, Collections.singletonList(pattern));
    String basePrincipal = null;
    if (matches != null && !matches.isEmpty()) {
      Set<String> fieldValues = matches.getOrDefault(pattern.getName(), Collections.emptySet());
      basePrincipal = fieldValues.stream().findFirst().orElse(null);
    }

    log.debug("Resolved basePrincipal: {}", basePrincipal);
    if (basePrincipal == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Resolved principal is null. "
              + "No principal information was found matching the configuration");
    }

    String principal = extractPrincipal(basePrincipal);
    log.debug("Resolved principal: {}", principal);
    return new BasicUserPrincipal(principal);
  }

  private String extractPrincipal(String str) {
    int start =
        startPattern.isEmpty() || !str.contains(startPattern)
            ? 0
            : str.indexOf(startPattern) + startPattern.length();
    int end = endPattern.isEmpty() ? str.length() : str.indexOf(endPattern, start);
    if (start >= 0 && end > start && end <= str.length()) {
      str = str.substring(start, end);
    }
    return str;
  }
}

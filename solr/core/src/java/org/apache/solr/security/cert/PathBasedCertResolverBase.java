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

import static org.apache.solr.security.cert.CertResolverPattern.matchesPattern;
import static org.apache.solr.security.cert.CertUtil.ISSUER_DN_PREFIX;
import static org.apache.solr.security.cert.CertUtil.SAN_PREFIX;
import static org.apache.solr.security.cert.CertUtil.SUBJECT_DN_PREFIX;

import java.lang.invoke.MethodHandles;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for implementing path-based certificate resolvers. This class provides common
 * functionality for extracting and processing certificate information based on configurable paths
 * and filters.
 */
public abstract class PathBasedCertResolverBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PARAM_NAME = "name";
  private static final String PARAM_PATH = "path";
  private static final String PARAM_FILTER = "filter";
  private static final String PARAM_FILTER_CHECK_TYPE = "checkType";
  private static final String PARAM_FILTER_VALUES = "values";

  /**
   * Creates a {@link CertResolverPattern} from a given configuration map and a default path. The
   * pattern defines how certificate information should be extracted and processed.
   *
   * @param config The configuration map containing resolver settings.
   * @param defaultPath The default path to use if none is specified in the configuration.
   * @return A new instance of {@link CertResolverPattern} based on the provided configuration.
   */
  @SuppressWarnings("unchecked")
  protected CertResolverPattern createCertResolverPattern(
      Map<String, Object> config, String defaultPath) {
    String path = ((String) config.getOrDefault(PARAM_PATH, defaultPath)).toLowerCase(Locale.ROOT);
    String name = ((String) config.getOrDefault(PARAM_NAME, path)).toLowerCase(Locale.ROOT);
    Map<String, Object> filter =
        (Map<String, Object>) config.getOrDefault(PARAM_FILTER, Collections.emptyMap());
    String checkType =
        (String)
            filter.getOrDefault(
                PARAM_FILTER_CHECK_TYPE, CertResolverPattern.CheckType.WILDCARD.toString());
    List<String> values =
        (List<String>) filter.getOrDefault(PARAM_FILTER_VALUES, Collections.emptyList());
    Set<String> lowerCaseValues =
        values.stream().map(value -> value.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    return new CertResolverPattern(name, path, checkType, lowerCaseValues);
  }

  /**
   * Extracts values from specified paths in a certificate and organizes them into a map. The map's
   * keys are derived from the names specified in the patterns, and the values are sets of strings
   * extracted from the certificate according to the pattern definitions.
   *
   * @param certificate The X509Certificate from which to extract information.
   * @param patterns A list of {@link CertResolverPattern} objects defining the extraction criteria.
   * @return A map where each key is a pattern name and each value is a set of extracted strings.
   * @throws CertificateParsingException If an error occurs while parsing the certificate.
   */
  protected Map<String, Set<String>> getValuesFromPaths(
      X509Certificate certificate, List<CertResolverPattern> patterns)
      throws CertificateParsingException {
    Map<String, Set<String>> fieldValuesMap = new HashMap<>();
    for (CertResolverPattern pattern : patterns) {
      String path = pattern.getPath();
      if (path.startsWith(SUBJECT_DN_PREFIX) || path.startsWith(ISSUER_DN_PREFIX)) {
        Optional<String> value =
            path.startsWith(SUBJECT_DN_PREFIX)
                ? CertUtil.extractFromSubjectDN(certificate, path)
                : CertUtil.extractFromIssuerDN(certificate, path);
        value.ifPresent(
            val ->
                fieldValuesMap
                    .computeIfAbsent(pattern.getName(), k -> new LinkedHashSet<>())
                    .add(val));
      } else if (path.startsWith(SAN_PREFIX)) {
        Optional<List<String>> sanValues =
            CertUtil.extractFromSAN(certificate, path, value -> matchesPattern(value, pattern));
        sanValues.ifPresent(
            values ->
                fieldValuesMap
                    .computeIfAbsent(pattern.getName(), k -> new LinkedHashSet<>())
                    .addAll(values));
      } else {
        throw new IllegalArgumentException(
            "Invalid path in the certificate resolver pattern: " + path);
      }
    }
    log.debug("Extracted field values: {}", fieldValuesMap);
    return fieldValuesMap;
  }
}

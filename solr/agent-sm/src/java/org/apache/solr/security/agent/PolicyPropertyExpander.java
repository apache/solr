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

import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Expands {@code ${property}} placeholders in policy file token values using system properties.
 *
 * <p>Throws {@link ExpandException} for any unresolved placeholder — fail-fast behaviour so that a
 * misconfigured policy (missing system property) is caught at startup rather than silently
 * resulting in an overly-permissive or broken policy.
 *
 * <p>Special cases:
 *
 * <ul>
 *   <li>{@code ${solr.zk.port}} defaults to {@code solr.port.listen + 1000} when absent, mirroring
 *       the ZooKeeper port convention used throughout Solr startup scripts.
 *   <li>{@code ${solr.data.home}} defaults to the value of {@code solr.solr.home} when not
 *       explicitly configured — matching Solr's default data layout.
 *   <li>{@code ${{literal}}} is left verbatim (escape hatch for values that must contain {@code
 *       ${...}}).
 * </ul>
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
class PolicyPropertyExpander {

  private PolicyPropertyExpander() {}

  /** Thrown when a {@code ${property}} placeholder cannot be resolved. */
  static class ExpandException extends Exception {
    ExpandException(String message) {
      super(message);
    }
  }

  // Matches ${{escaped}} (literal pass-through) or ${normal} (property lookup)
  private static final Pattern PLACEHOLDER_PATTERN =
      Pattern.compile("\\$\\{\\{(?<escaped>.*?)}}|\\$\\{(?<normal>.*?)}");

  /**
   * Non-standard env var names for sysprops that do not follow the SOLR_FOO_BAR convention. Mirrors
   * the custom entries in {@code EnvToSyspropMappings.properties} in SolrJ.
   */
  private static final Map<String, String> CUSTOM_ENV_NAMES =
      Map.of(
          "solr.solr.home", "SOLR_HOME",
          "solr.install.dir", "SOLR_TIP",
          "solr.install.symDir", "SOLR_TIP_SYM");

  /**
   * Returns the value for {@code sysprop} by checking, in order:
   *
   * <ol>
   *   <li>{@code System.getProperty(sysprop)}
   *   <li>The env var derived from the sysprop name (standard {@code SOLR_FOO_BAR} convention, with
   *       custom overrides for non-standard names)
   * </ol>
   *
   * Returns {@code null} if neither source has a value.
   */
  static String getPropertyOrEnv(String sysprop) {
    String val = System.getProperty(sysprop);
    if (val != null && !val.isBlank()) return val.trim();
    String envVar =
        CUSTOM_ENV_NAMES.getOrDefault(sysprop, sysprop.toUpperCase(Locale.ROOT).replace('.', '_'));
    val = System.getenv(envVar);
    return (val != null && !val.isBlank()) ? val.trim() : null;
  }

  /**
   * Expands all {@code ${property}} placeholders in {@code value}. Returns {@code null} if {@code
   * value} is {@code null}.
   *
   * @throws ExpandException if a placeholder references a system property that is not set
   */
  static String expand(String value) throws ExpandException {
    if (value == null || !value.contains("${")) {
      return value;
    }
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(value);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String replacement = handleMatch(matcher);
      matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  private static String handleMatch(Matcher match) throws ExpandException {
    String escaped = match.group("escaped");
    if (escaped != null) {
      // ${{literal}} — return verbatim
      return "${{" + escaped + "}}";
    }
    return expandPlaceholder(match.group("normal"));
  }

  private static String expandPlaceholder(String placeholder) throws ExpandException {
    // solr.zk.port is optional — default to solr.port.listen + 1000
    if ("solr.zk.port".equals(placeholder)) {
      String val = getPropertyOrEnv("solr.zk.port");
      if (val != null) return val;
      String solrPort = getPropertyOrEnv("solr.port.listen");
      if (solrPort == null) solrPort = "8983";
      try {
        return String.valueOf(Integer.parseInt(solrPort.trim()) + 1000);
      } catch (NumberFormatException e) {
        return "9983";
      }
    }
    // solr.install.symDir is optional — falls back to solr.install.dir when absent.
    if ("solr.install.symDir".equals(placeholder)) {
      String val = getPropertyOrEnv("solr.install.symDir");
      if (val != null) return val;
      val = getPropertyOrEnv("solr.install.dir");
      if (val != null) return val;
      throw new ExpandException(
          "Unresolved policy variable: ${solr.install.symDir} (and ${solr.install.dir} is also unset)");
    }
    // solr.data.home is optional — when not configured, Solr stores data under solr.solr.home
    if ("solr.data.home".equals(placeholder)) {
      String val = getPropertyOrEnv("solr.data.home");
      if (val != null) return val;
      val = getPropertyOrEnv("solr.solr.home");
      if (val != null) return val;
      throw new ExpandException(
          "Unresolved policy variable: ${solr.data.home} (and ${solr.solr.home} is also unset)");
    }
    String value = getPropertyOrEnv(placeholder);
    if (value == null) {
      throw new ExpandException("Unresolved policy variable: ${" + placeholder + "}");
    }
    return value;
  }
}

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

package org.apache.solr.util;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class RedactionUtils {
  public static final String SOLR_REDACTION_SYSTEM_PATTERN_PROP = "solr.redaction.system.pattern";
  private static Pattern pattern =
      Pattern.compile(
          System.getProperty(SOLR_REDACTION_SYSTEM_PATTERN_PROP, ".*password.*"),
          Pattern.CASE_INSENSITIVE);
  private static final String REDACT_STRING = "--REDACTED--";
  public static final String NODE_REDACTION_PREFIX = "N_";
  public static final String COLL_REDACTION_PREFIX = "COLL_";

  private static boolean redactSystemProperty =
      Boolean.parseBoolean(System.getProperty("solr.redaction.system.enabled", "true"));

  /**
   * Returns if the given system property should be redacted.
   *
   * @param name The system property that is being checked.
   * @return true if property should be redacted.
   */
  public static boolean isSystemPropertySensitive(String name) {
    return redactSystemProperty && pattern.matcher(name).matches();
  }

  /**
   * @return redaction string to be used instead of the value.
   */
  public static String getRedactString() {
    return REDACT_STRING;
  }

  public static void setRedactSystemProperty(boolean redactSystemProperty) {
    RedactionUtils.redactSystemProperty = redactSystemProperty;
  }

  /**
   * Replace actual names found in a string with redacted names.
   *
   * @param redactions a map of original to redacted names
   * @param data string to redact
   * @return redacted string where all actual names have been replaced.
   */
  public static String redactNames(Map<String, String> redactions, String data) {
    // replace the longest first to avoid partial replacements
    Map<String, String> sorted =
        new TreeMap<>(
            Comparator.comparing(String::length).reversed().thenComparing(String::compareTo));
    sorted.putAll(redactions);
    for (Map.Entry<String, String> entry : sorted.entrySet()) {
      data = data.replaceAll("\\Q" + entry.getKey() + "\\E", entry.getValue());
    }
    return data;
  }
}

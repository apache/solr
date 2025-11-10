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
package org.apache.solr.crossdc.common;

import java.util.Locale;
import java.util.Map;

/**
 * Helper functionality related to redacting arbitrary properties which may contain sensitive
 * password information.
 *
 * <p>Used primarily as a safeguard before logging out configuration properties. Redaction logic
 * heavily depends on string matching against elements of the {@link
 * #PATTERNS_REQUIRING_REDACTION_LOWERCASE} block-list.
 */
public class SensitivePropRedactionUtils {
  private static final String[] PATTERNS_REQUIRING_REDACTION_LOWERCASE =
      new String[] {"password", "credentials"};
  private static final String REDACTED_STRING = "<REDACTED>";

  public static boolean propertyRequiresRedaction(String propName) {
    final String propNameLowercase = propName.toLowerCase(Locale.ROOT);
    for (String pattern : PATTERNS_REQUIRING_REDACTION_LOWERCASE) {
      if (propNameLowercase.contains(pattern)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Redacts a property value if necessary, and returns the result.
   *
   * <p>Redaction only occurs when the property name matches a pattern on the {@link
   * #PATTERNS_REQUIRING_REDACTION_LOWERCASE} block-list.
   *
   * @param propName the name or key of the property being considered for redaction
   * @param propValue the value of the property under consideration; returned verbatim if redaction
   *     is not necessary.
   */
  public static String redactPropertyIfNecessary(String propName, String propValue) {
    return propertyRequiresRedaction(propName) ? REDACTED_STRING : propValue;
  }

  public static String flattenAndRedactForLogging(Map<String, Object> properties) {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      final Object printablePropValue =
          redactPropertyIfNecessary(entry.getKey(), String.valueOf(entry.getValue()));
      sb.append(entry.getKey()).append("=").append(printablePropValue).append(", ");
    }
    sb.append("}");
    return sb.toString();
  }
}

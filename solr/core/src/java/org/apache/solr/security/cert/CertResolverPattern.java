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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a pattern for resolving certificate information, specifying the criteria for
 * extracting and matching values from certificates.
 */
public class CertResolverPattern {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String name;
  private final String path;
  private final CheckType checkType;
  private final Set<String> filterValues;

  /**
   * Constructs a CertResolverPattern with specified parameters.
   *
   * @param name The name associated with this pattern.
   * @param path The certificate field path this pattern applies to.
   * @param checkType The type of check to perform on extracted values.
   * @param filterValues The set of values to check against the extracted certificate field.
   */
  public CertResolverPattern(String name, String path, String checkType, Set<String> filterValues) {
    this.name = name;
    this.path = path;
    this.checkType = CheckType.fromString(checkType);
    this.filterValues = filterValues;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public CheckType getCheckType() {
    return checkType;
  }

  public Set<String> getFilterValues() {
    return filterValues;
  }

  public static boolean matchesPattern(String value, CertResolverPattern pattern) {
    return matchesPattern(value, pattern.getCheckType(), pattern.getFilterValues());
  }

  /**
   * Determines if a given value matches the specified filter values, depending on the check type.
   *
   * @param value The value to check.
   * @param checkType The type of check to perform.
   * @param values The set of values to check against.
   * @return True if the value matches the criteria; false otherwise.
   */
  public static boolean matchesPattern(String value, CheckType checkType, Set<String> values) {
    log.debug("matchesPattern   value:{}   checkType:{}    values:{}", value, checkType, values);
    String lowerValue =
        value.toLowerCase(Locale.ROOT); // lowercase for case-insensitive comparisons
    switch (checkType) {
      case EQUALS:
        return values.contains(lowerValue);
      case STARTS_WITH:
        return values.stream().anyMatch(lowerValue::startsWith);
      case ENDS_WITH:
        return values.stream().anyMatch(lowerValue::endsWith);
      case CONTAINS:
        return values.stream().anyMatch(lowerValue::contains);
      case WILDCARD:
        return true;
      default:
        return false;
    }
  }

  /** Enum defining the types of checks that can be performed on extracted certificate values. */
  public enum CheckType {
    STARTS_WITH,
    ENDS_WITH,
    CONTAINS,
    EQUALS,
    WILDCARD;
    // TODO: add regex support

    private static final Map<String, CheckType> lookup =
        Map.of(
            "equals", EQUALS,
            "startswith", STARTS_WITH,
            "endswith", ENDS_WITH,
            "contains", CONTAINS,
            "*", WILDCARD,
            "wildcard", WILDCARD);

    public static CheckType fromString(String checkType) {
      if (checkType == null) {
        throw new IllegalArgumentException("CheckType cannot be null");
      }
      CheckType result = lookup.get(checkType.toLowerCase(Locale.ROOT));
      if (result == null) {
        throw new IllegalArgumentException("No CheckType with text '" + checkType + "' found");
      }
      return result;
    }
  }
}

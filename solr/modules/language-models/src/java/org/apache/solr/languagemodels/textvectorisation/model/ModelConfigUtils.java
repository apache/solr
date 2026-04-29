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
package org.apache.solr.languagemodels.textvectorisation.model;

public class ModelConfigUtils {

  /** Convert JSON-parsed values to the expected parameter type */
  public static Object convertValue(Object value, Class<?> targetType) {
    if (value == null) return null;

    if (targetType.isAssignableFrom(value.getClass())) {
      return value;
    }

    // Handle common type conversions from JSON parsing
    if (targetType == int.class || targetType == Integer.class) {
      if (value instanceof Long) return ((Long) value).intValue();
      if (value instanceof String) return Integer.parseInt((String) value);
    }
    if (targetType == long.class || targetType == Long.class) {
      if (value instanceof Integer) return ((Integer) value).longValue();
      if (value instanceof String) return Long.parseLong((String) value);
    }
    if (targetType == double.class || targetType == Double.class) {
      if (value instanceof Number) return ((Number) value).doubleValue();
      if (value instanceof String) return Double.parseDouble((String) value);
    }
    if (targetType == float.class || targetType == Float.class) {
      if (value instanceof Number) return ((Number) value).floatValue();
      if (value instanceof String) return Float.parseFloat((String) value);
    }
    if (targetType == boolean.class || targetType == Boolean.class) {
      if (value instanceof String) return Boolean.parseBoolean((String) value);
    }
    if (targetType == String.class) {
      return value.toString();
    }

    return value;
  }
}

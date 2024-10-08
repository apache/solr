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

import java.util.Map;

public class ConfigProperty {

  private final String key;
  private final String defaultValue;

  private boolean required = false;

  public ConfigProperty(String key, String defaultValue, boolean required) {
    this.key = key;
    this.defaultValue = defaultValue;
    this.required = required;
  }

  public ConfigProperty(String key, String defaultValue) {
    this.key = key;
    this.defaultValue = defaultValue;
  }

  public ConfigProperty(String key) {
    this.key = key;
    this.defaultValue = null;
  }

  public String getKey() {
    return key;
  }

  public boolean isRequired() {
    return required;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public String getValue(Map<?, ?> properties) {
    String val = (String) properties.get(key);
    if (val == null) {
      return defaultValue;
    }
    return val;
  }

  public Integer getValueAsInt(Map<?, ?> properties) {
    Object value = properties.get(key);
    if (value != null) {
      if (value instanceof Integer) {
        return (Integer) value;
      }
      return Integer.parseInt(value.toString());
    }
    if (defaultValue == null) {
      return null;
    }
    return Integer.parseInt(defaultValue);
  }

  public Boolean getValueAsBoolean(Map<?, ?> properties) {
    Object value = properties.get(key);
    if (value != null) {
      if (value instanceof Boolean) {
        return (Boolean) value;
      }
      return Boolean.parseBoolean(value.toString());
    }
    return Boolean.parseBoolean(defaultValue);
  }
}

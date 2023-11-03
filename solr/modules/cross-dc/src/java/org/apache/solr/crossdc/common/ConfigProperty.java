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

  public String getValue(Map properties) {
    String val = (String) properties.get(key);
    if (val == null) {
     return defaultValue;
    }
    return val;
  }

  public Integer getValueAsInt(Map properties) {
    Object value = (Object) properties.get(key);
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

  public Boolean getValueAsBoolean(Map properties) {
    Object value = (Object) properties.get(key);
    if (value != null) {
      if (value instanceof Boolean) {
        return (Boolean) value;
      }
      return Boolean.parseBoolean(value.toString());
    }
    return Boolean.parseBoolean(defaultValue);
  }
}

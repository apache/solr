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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

/**
 * This class is a unified provider of environment variables and system properties. It exposes a
 * mutable copy of environment variables with. It also converts 'SOLR_FOO' variables to system
 * properties 'solr.foo' and provide various convenience accessors for them.
 */
public class EnvUtils {
  private static final SortedMap<String, String> ENV = new TreeMap<>(System.getenv());
  private static final Map<String, String> CUSTOM_MAPPINGS = new HashMap<>();

  static {
    try {
      Properties props = new Properties();
      try (InputStream stream =
          EnvUtils.class.getClassLoader().getResourceAsStream("EnvToSyspropMappings.properties")) {
        props.load(new InputStreamReader(Objects.requireNonNull(stream), StandardCharsets.UTF_8));
        for (String key : props.stringPropertyNames()) {
          CUSTOM_MAPPINGS.put(key, props.getProperty(key));
        }
        init(false);
      }
    } catch (IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.INVALID_STATE, "Failed loading env.var->properties mapping", e);
    }
  }

  /**
   * Get Solr's mutable copy of all environment variables.
   *
   * @return sorted map of environment variables
   */
  public static SortedMap<String, String> getEnvs() {
    return ENV;
  }

  /** Get a single environment variable as string */
  public static String getEnv(String key) {
    return ENV.get(key);
  }

  /** Get a single environment variable as string, or default */
  public static String getEnv(String key, String defaultValue) {
    return ENV.getOrDefault(key, defaultValue);
  }

  /** Get an environment variable as long */
  public static long getEnvAsLong(String key) {
    return Long.parseLong(ENV.get(key));
  }

  /** Get an environment variable as long, or default value */
  public static long getEnvAsLong(String key, long defaultValue) {
    String value = ENV.get(key);
    if (value == null) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }

  /** Get an env var as boolean */
  public static boolean getEnvAsBool(String key) {
    return Boolean.parseBoolean(ENV.get(key));
  }

  /** Get an env var as boolean, or default value */
  public static boolean getEnvAsBool(String key, boolean defaultValue) {
    String value = ENV.get(key);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }

  /** Get comma separated strings from env as List */
  public static List<String> getEnvAsList(String key) {
    return getEnv(key) != null ? stringValueToList(getEnv(key)) : null;
  }

  /** Get comma separated strings from env as List */
  public static List<String> getEnvAsList(String key, List<String> defaultValue) {
    return ENV.get(key) != null ? getEnvAsList(key) : defaultValue;
  }

  /** Set an environment variable */
  public static void setEnv(String key, String value) {
    ENV.put(key, value);
  }

  /** Set all environment variables */
  public static void setEnvs(Map<String, String> env) {
    ENV.clear();
    ENV.putAll(env);
  }

  /** Get all Solr system properties as a sorted map */
  public static SortedMap<String, String> getProps() {
    SortedMap<String, String> props = new TreeMap<>();
    for (String key : System.getProperties().stringPropertyNames()) {
      props.put(key, System.getProperty(key));
    }
    return props;
  }

  /** Get a property as string */
  public static String getProp(String key) {
    return getProp(key, null);
  }

  /** Get a property as string */
  public static String getProp(String key, String defaultValue) {
    return System.getProperties().getProperty(key, defaultValue);
  }

  /** Get property as integer */
  public static long getPropAsLong(String key) {
    return Long.parseLong(getProp(key));
  }

  /** Get property as long, or default value */
  public static long getPropAsLong(String key, long defaultValue) {
    String value = getProp(key);
    if (value == null) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }

  /** Get property as boolean */
  public static boolean getPropAsBool(String key) {
    return Boolean.parseBoolean(getProp(key));
  }

  /** Get property as boolean, or default value */
  public static boolean getPropAsBool(String key, boolean defaultValue) {
    String value = getProp(key);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }

  /**
   * Get comma separated strings from sysprop as List
   *
   * @return list of strings, or null if not found
   */
  public static List<String> getPropAsList(String key) {
    return getProp(key) != null ? stringValueToList(getProp(key)) : null;
  }

  /**
   * Get comma separated strings from sysprop as List, or default value
   *
   * @return list of strings, or provided default if not found
   */
  public static List<String> getPropAsList(String key, List<String> defaultValue) {
    return getProp(key) != null ? getPropAsList(key) : defaultValue;
  }

  /** Set a system property. Shim to {@link System#setProperty(String, String)} */
  public static void setProp(String key, String value) {
    System.setProperty(key, value);
  }

  /**
   * Re-reads environment variables and updates the internal map.
   *
   * @param overwrite if true, overwrite existing system properties with environment variables
   */
  public static void init(boolean overwrite) {
    // Convert eligible environment variables with SOLR_ prefix to system properties
    for (String key :
        ENV.keySet().stream().filter(k -> k.startsWith("SOLR_")).toArray(String[]::new)) {
      String sysPropKey = envNameToSyspropName(key);
      // Existing system properties take precedence
      if (!sysPropKey.isBlank() && (overwrite || getProp(sysPropKey) == null)) {
        setProp(sysPropKey, ENV.get(key));
      }
    }
  }

  protected static String envNameToSyspropName(String envName) {
    return CUSTOM_MAPPINGS.containsKey(envName)
        ? CUSTOM_MAPPINGS.get(envName)
        : envName.toLowerCase(Locale.ROOT).replace("_", ".");
  }

  @SuppressWarnings("unchecked")
  private static List<String> stringValueToList(String string) {
    if (string.startsWith("[") && string.endsWith("]")) {
      // Convert a JSON string to a List<String> using Noggit parser
      return (List<String>) Utils.fromJSONString(string);
    } else {
      return StrUtils.splitSmart(string, ",", true).stream()
          .map(String::trim)
          .collect(Collectors.toList());
    }
  }
}

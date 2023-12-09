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
 * mutable copy of the environment variables. It also converts 'SOLR_FOO' variables to system
 * properties 'solr.foo' and provide various convenience accessors for them.
 */
public class EnvUtils {
  private static final SortedMap<String, String> ENV = new TreeMap<>(System.getenv());
  private static final Map<String, String> CUSTOM_MAPPINGS = new HashMap<>();
  private static Map<String, String> camelCaseToDotsMap = new HashMap<>();
  private static boolean initialized = false;

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
    return StrUtils.parseBool(ENV.get(key));
  }

  /** Get an env var as boolean, or default value */
  public static boolean getEnvAsBool(String key, boolean defaultValue) {
    String value = ENV.get(key);
    if (value == null) {
      return defaultValue;
    }
    return StrUtils.parseBool(value);
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
  public static synchronized void setEnvs(Map<String, String> env) {
    ENV.clear();
    ENV.putAll(env);
  }

  /** Get all Solr system properties as a sorted map */
  public static SortedMap<String, String> getProps() {
    ensureInitialized();
    return System.getProperties().entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> entry.getValue().toString(),
                (e1, e2) -> e1,
                TreeMap::new));
  }

  /** Get a property as string */
  public static String getProp(String key) {
    return getProp(key, null);
  }

  /**
   * Get a property as string with a fallback value. All other getProp* methods use this.
   *
   * @param key property key, which treats 'camelCase' the same as 'camel.case'
   */
  public static String getProp(String key, String defaultValue) {
    ensureInitialized(); // Avoid race condition with init()
    String value = getPropWithCamelCaseFallback(key);
    return value != null ? value : defaultValue;
  }

  /**
   * Get a property from given key or an alias key converted from CamelCase to dot separated.
   *
   * @return property value or value of dot-separated alias key or null if not found
   */
  private static String getPropWithCamelCaseFallback(String key) {
    String value = System.getProperty(key);
    if (value != null) {
      return value;
    } else {
      // Figure out if string is CamelCase and convert to dot separated
      String altKey = camelCaseToDotSeparated(key);
      return System.getProperty(altKey);
    }
  }

  private static String camelCaseToDotSeparated(String key) {
    if (camelCaseToDotsMap.containsKey(key)) {
      return camelCaseToDotsMap.get(key);
    } else {
      String converted =
          String.join(".", key.split("(?=[A-Z])")).replace("..", ".").toLowerCase(Locale.ROOT);
      camelCaseToDotsMap.put(key, converted);
      return converted;
    }
  }

  /** Get property as integer */
  public static Long getPropAsLong(String key) {
    return getPropAsLong(key, null);
  }

  /** Get property as long, or default value */
  public static Long getPropAsLong(String key, Long defaultValue) {
    String value = getProp(key);
    if (value == null) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }

  /** Get property as boolean */
  public static Boolean getPropAsBool(String key) {
    return getPropAsBool(key, null);
  }

  /** Get property as boolean, or default value */
  public static Boolean getPropAsBool(String key, Boolean defaultValue) {
    String value = getProp(key);
    if (value == null) {
      return defaultValue;
    }
    return StrUtils.parseBool(value);
  }

  /**
   * Get comma separated strings from sysprop as List
   *
   * @return list of strings, or null if not found
   */
  public static List<String> getPropAsList(String key) {
    return getPropAsList(key, null);
  }

  /**
   * Get comma separated strings from sysprop as List, or default value
   *
   * @return list of strings, or provided default if not found
   */
  public static List<String> getPropAsList(String key, List<String> defaultValue) {
    return getProp(key) != null ? stringValueToList(getProp(key)) : defaultValue;
  }

  /** Set a system property. Shim to {@link System#setProperty(String, String)} */
  public static void setProp(String key, String value) {
    System.setProperty(key, value);
    System.setProperty(camelCaseToDotSeparated(key), value);
  }

  /**
   * Re-reads environment variables and updates the internal map. Mainly for internal and test use.
   *
   * @param overwrite if true, overwrite existing system properties with environment variables
   */
  static synchronized void init(boolean overwrite) {
    // Convert eligible environment variables to system properties
    for (String key : ENV.keySet().toArray(String[]::new)) {
      if (key.startsWith("SOLR_") || CUSTOM_MAPPINGS.containsKey(key)) {
        String sysPropKey = envNameToSyspropName(key);
        // Existing system properties take precedence
        if (!sysPropKey.isBlank() && (overwrite || getProp(sysPropKey) == null)) {
          setProp(sysPropKey, ENV.get(key));
        }
      }
    }
    initialized = true;
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

  private static synchronized void ensureInitialized() {
    while (!initialized) {
      try {
        //noinspection BusyWait
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "EnvUtils not initialized", e);
      }
    }
  }
}

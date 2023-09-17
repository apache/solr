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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.solr.common.util.StrUtils;

/**
 * This class is a unified provider of environment variables and system properties. It exposes a
 * mutable copy of system environment variables with SOLR_ prefix. It also converts such variables
 * to system properties and provide various convenience accessors for them.
 */
public class EnvUtils {
  private static final SortedMap<String, String> ENV = new TreeMap<>(System.getenv());
  private static final Map<String, String> CUSTOM_MAPPINGS = new HashMap<>();
  private static final List<String> DO_NOT_MAP =
      List.of(
          "SOLR_SERVER_DIR",
          "SOLR_SSL_KEY_STORE_PASSWORD",
          "SOLR_SSL_TRUST_STORE_PASSWORD",
          "SOLR_SSL_CLIENT_KEY_STORE_PASSWORD",
          "SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD",
          "SOLR_STOP_WAIT",
          "SOLR_START_WAIT",
          "SOLR_HEAP",
          "SOLR_JAVA_MEM",
          "GC_LOG_OPTS",
          "SOLR_JAVA_STACK_SIZE",
          "SOLR_OPTS",
          "SOLR_ADDL_ARGS",
          "SOLR_JETTY_CONFIG",
          "SOLR_REQUESTLOG_ENABLED",
          "SOLR_AUTHENTICATION_CLIENT_BUILDER",
          "SOLR_SSL_OPTS",
          "SOLR_OPTS_INTERNAL",
          "SOLR_HEAP_DUMP",
          "SOLR_HEAP_DUMP_DIR",
          "SOLR_INCLUDE",
          "SOLR_LOG_LEVEL_OPT");

  static {
    CUSTOM_MAPPINGS.putAll(
        Map.of(
            "SOLR_SSL_KEY_STORE_TYPE", "solr.jetty.keystore.type",
            "SOLR_SSL_TRUST_STORE", "solr.jetty.truststore",
            "SOLR_SSL_TRUST_STORE_TYPE", "solr.jetty.truststore.type",
            "SOLR_SSL_NEED_CLIENT_AUTH", "solr.jetty.ssl.needClientAuth",
            "SOLR_SSL_WANT_CLIENT_AUTH", "solr.jetty.ssl.wantClientAuth",
            "SOLR_SSL_CLIENT_KEY_STORE", "javax.net.ssl.keyStore",
            "SOLR_SSL_CLIENT_KEY_STORE_TYPE", "javax.net.ssl.keyStoreType",
            "SOLR_SSL_CLIENT_TRUST_STORE", "javax.net.ssl.trustStore",
            "SOLR_SSL_CLIENT_TRUST_STORE_TYPE", "javax.net.ssl.trustStoreType"));
    CUSTOM_MAPPINGS.putAll(
        Map.of(
            "SOLR_HOME", "solr.solr.home",
            "SOLR_PORT", "jetty.port",
            "SOLR_HOST", "host",
            "SOLR_LOGS_DIR", "solr.log.dir",
            "SOLR_TIMEZONE", "user.timezone",
            "SOLR_TIP", "solr.install.dir",
            "SOLR_TIP_SYM", "solr.install.symDir",
            "DEFAULT_CONFDIR", "solr.default.confdir",
            "SOLR_HADOOP_CREDENTIAL_PROVIDER_PATH", "hadoop.security.credential.provider.path",
            "SOLR_SSL_KEY_STORE", "solr.jetty.keystore"));
    CUSTOM_MAPPINGS.putAll(
        Map.of(
            "SOLR_WAIT_FOR_ZK", "waitForZk",
            "SOLR_DELETE_UNKNOWN_CORES", "solr.deleteUnknownCore",
            "SOLR_ENABLE_REMOTE_STREAMING", "solr.enableRemoteStreaming",
            "SOLR_ENABLE_STREAM_BODY", "solr.enableStreamBody"));
    init(false);
  }

  /**
   * Get all environment variables with SOLR_ prefix.
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
  public static List<String> getEnvCommaSepAsList(String key) {
    return StrUtils.splitSmart(ENV.get(key), ',', true);
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
    return System.getProperties().getProperty(key);
  }

  /** Get a property as string */
  public static String getProp(String key, String defaultValue) {
    return System.getProperties().getProperty(key, defaultValue);
  }

  /** Get property as integer */
  public static long getPropAsLong(String key) {
    return Long.parseLong(System.getProperties().getProperty(key));
  }

  /** Get property as long, or default value */
  public static long getPropAsLong(String key, long defaultValue) {
    String value = System.getProperties().getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }

  /** Get property as boolean */
  public static boolean getPropAsBool(String key) {
    return Boolean.parseBoolean(System.getProperties().getProperty(key));
  }

  /** Get property as boolean, or default value */
  public static boolean getPropAsBool(String key, boolean defaultValue) {
    String value = System.getProperties().getProperty(key);
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
  public static List<String> getPropCommaSepAsList(String key) {
    return getProp(key) != null ? StrUtils.splitSmart(getProp(key), ",", true) : null;
  }

  /**
   * Get comma separated strings from sysprop as List, or default value
   *
   * @return list of strings, or provided default if not found
   */
  public static List<String> getPropCommaSepAsList(String key, List<String> defaultValue) {
    return getProp(key) != null ? StrUtils.splitSmart(getProp(key), ",", true) : defaultValue;
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
    // convert all environment variables with SOLR_ prefix to system properties
    for (String key :
        ENV.keySet().stream().filter(k -> !DO_NOT_MAP.contains(k)).toArray(String[]::new)) {
      String sysPropKey =
          CUSTOM_MAPPINGS.containsKey(key) ? CUSTOM_MAPPINGS.get(key) : envNameToSyspropName(key);
      // Existing system properties take precedence
      if (overwrite || getProp(sysPropKey) == null) {
        setProp(sysPropKey, ENV.get(key));
      }
    }
  }

  protected static String envNameToSyspropName(String envName) {
    return envName.toLowerCase(Locale.ROOT).replace("_", ".");
  }
}

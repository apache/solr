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

package org.apache.solr.common.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.LogListener;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnvUtilsTest extends SolrTestCase {

  private static final Map<String, String> ENV =
      Map.ofEntries(
          Map.entry("SOLR_HOME", "/home/solr"),
          Map.entry("SOLR_PORT_LISTEN", "8983"),
          Map.entry("SOLR_HOST_ADVERTISE", "localhost"),
          Map.entry("SOLR_LOG_LEVEL", "INFO"),
          Map.entry("SOLR_BOOLEAN", "true"),
          Map.entry("SOLR_LONG", "1234567890"),
          Map.entry("SOLR_COMMASEP", "one,two, three"),
          Map.entry("SOLR_JSON_LIST", "[\"one\", \"two\", \"three\"]"),
          Map.entry("SOLR_ALWAYS_ON_TRACE_ID", "true"),
          Map.entry("SOLR_STR_WITH_NEWLINE", "foo\nbar,baz"),
          Map.entry("SOLR_TIP", "/opt/solr"),
          Map.entry("SOLR_TIP_SYM", "/opt/solr-9.9.9"));

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Make a map of some common Solr environment variables for testing, and initialize EnvUtils
    EnvUtils.init(true, ENV, System.getProperties());
  }

  @Test
  public void testGetProp() {
    assertEquals("INFO", EnvUtils.getProperty("solr.log.level"));

    assertNull(EnvUtils.getProperty("solr.nonexist"));
    assertEquals("myString", EnvUtils.getProperty("solr.nonexist", "myString"));

    assertTrue(EnvUtils.getPropertyAsBool("solr.boolean"));
    assertFalse(EnvUtils.getPropertyAsBool("solr.boolean.nonexist", false));

    assertEquals("1234567890", EnvUtils.getProperty("solr.long"));
    assertEquals(Long.valueOf(1234567890L), EnvUtils.getPropertyAsLong("solr.long"));
    assertEquals(Long.valueOf(987L), EnvUtils.getPropertyAsLong("solr.long.nonexist", 987L));

    assertEquals("one,two, three", EnvUtils.getProperty("solr.commasep"));
    assertEquals(List.of("one", "two", "three"), EnvUtils.getPropertyAsList("solr.commasep"));
    assertEquals(List.of("one", "two", "three"), EnvUtils.getPropertyAsList("solr.json.list"));
    assertEquals(
        List.of("fallback"), EnvUtils.getPropertyAsList("SOLR_MISSING", List.of("fallback")));
  }

  @Test
  public void getPropWithCamelCase() {
    assertEquals("INFO", EnvUtils.getProperty("solr.logLevel"));
    assertEquals("INFO", EnvUtils.getProperty("solr.LogLevel"));
    assertEquals(Long.valueOf(1234567890L), EnvUtils.getPropertyAsLong("solrLong"));
    assertEquals(Boolean.TRUE, EnvUtils.getPropertyAsBool("solr.tracing.always.on.enabled"));
  }

  @Test
  public void testEnvsWithCustomKeyNameMappings() {
    // These map to a sysprop name that doesn't follow the standard SOLR_FOO_BAR -> solr.foo.bar
    // convention (see EnvToSyspropMappings.properties). Assert against literal expected values,
    // not ENV.get(...), so a broken/missing mapping would actually be caught here.
    assertEquals("/home/solr", EnvUtils.getProperty("solr.solr.home"));
    assertEquals("/opt/solr", EnvUtils.getProperty("solr.install.dir"));
    assertEquals("/opt/solr-9.9.9", EnvUtils.getProperty("solr.install.symDir"));
  }

  @Test
  public void testNotMapped() {
    assertFalse(EnvUtils.getProperties().containsKey("solr.ssl.key.store.password"));
    assertFalse(EnvUtils.getProperties().containsKey("gc.log.opts"));
  }

  @Test
  public void testOverwrite() {
    EnvUtils.setProperty("solr.overwrite", "original");
    var env2 = Map.of("SOLR_OVERWRITE", "overwritten");
    EnvUtils.init(false, env2, new Properties());
    assertEquals("original", EnvUtils.getProperty("solr.overwrite"));
    EnvUtils.init(true, env2, new Properties());
    assertEquals("overwritten", EnvUtils.getProperty("solr.overwrite"));
  }

  @Test
  public void testDeprecated() {
    var env = Map.of("SOLR_OVERWRITE", "overwritten");
    Properties defaultProps = new Properties();
    defaultProps.setProperty("solr.config.set.forbidden.file.types", "xml,json,jar");

    EnvUtils.init(false, env, defaultProps);
    assertEquals("xml,json,jar", EnvUtils.getProperty("solr.configset.forbidden.file.types"));
  }

  @Test
  public void deprecatedCamelCaseSystemPropertyIsMigratedToCurrentName() {
    Properties sysprops = new Properties();
    sysprops.setProperty("solr.auth.jwt.allowOutboundHttp", "true");
    EnvUtils.init(false, Map.of(), sysprops);
    assertTrue(EnvUtils.getPropertyAsBool("solr.auth.jwt.outbound.http.enabled"));
  }

  @Test
  public void deprecatedCamelCaseOldNameInMappingsFileIsTranslated() {
    Properties sysprops = new Properties();
    sysprops.setProperty("collection.configName", "techproducts");
    EnvUtils.init(false, Map.of(), sysprops);
    assertEquals("techproducts", EnvUtils.getProperty("solr.configset.bootstrap.config.name"));
  }

  @Test
  public void deprecatedCamelCaseInvertedPropertyIsTranslatedAndValueIsFlipped() {
    Properties sysprops = new Properties();
    sysprops.setProperty("solr.disableFingerprint", "true");
    EnvUtils.init(true, Map.of(), sysprops);
    assertFalse(EnvUtils.getPropertyAsBool("solr.index.replication.fingerprint.enabled"));
  }

  @Test
  public void testFlippingDisabledToEnabledPropertyName() {

    var env = Map.of("SOLR_ADMIN_UI_DISABLED", "true");
    Properties defaultProps = new Properties();
    defaultProps.setProperty("solr.admin.ui.disabled", "true");

    EnvUtils.init(false, env, defaultProps);
    assertEquals(false, EnvUtils.getPropertyAsBool("solr.ui.enabled"));
  }

  /**
   * These env vars must map directly to their current sysprop name, not to a legacy/intermediate
   * name that DeprecatedSystemPropertyMappings.properties also treats as deprecated -- otherwise
   * EnvUtils' own deprecation-forwarding logic trips on itself and logs a confusing warning, even
   * though the value still resolves correctly via that indirection. A value-only assertion wouldn't
   * catch a regression here, since the value resolves fine either way -- the warning is the actual
   * symptom, so this asserts on both.
   *
   * <p>SOLR_ALWAYS_ON_TRACE_ID is the same pattern (see {@link #getPropWithCamelCase}) but is
   * deliberately excluded here: it shares a target sysprop with that other test, and this test
   * would clobber it with a non-boolean value depending on random test execution order.
   *
   * <p>The LogListener is scoped to only these six properties' names (rather than listening for
   * *any* WARN from EnvUtils) because {@code init()} is called here with the real, live {@code
   * System.getProperties()} -- its deprecated-property-forwarding loop rescans *all* current system
   * properties every time, so leftover deprecated markers set by unrelated tests earlier in this
   * same suite/JVM (e.g. {@link #testFlippingDisabledToEnabledPropertyName}) would otherwise be
   * re-detected and re-warned-about here too, causing flaky, unrelated failures.
   */
  @Test
  public void envToSyspropMappingsDoNotTriggerDeprecationWarnings() {
    var envVarToExpectedSysprop =
        Map.of(
            "ZK_CLIENT_TIMEOUT", "solr.zookeeper.client.timeout",
            "ZK_CREATE_CHROOT", "solr.zookeeper.chroot.create",
            "SOLR_AUTH_JWT_ALLOW_OUTBOUND_HTTP", "solr.auth.jwt.outbound.http.enabled",
            "SOLR_HIDDEN_SYS_PROPS", "solr.responses.hidden.sys.props",
            "SOLR_ALLOW_PATHS", "solr.security.allow.paths",
            "SOLR_ALLOW_URLS", "solr.security.allow.urls");
    var onlyOurTargets =
        Pattern.compile(
            envVarToExpectedSysprop.values().stream()
                .map(Pattern::quote)
                .collect(Collectors.joining("|")));

    try (LogListener warnLog = LogListener.warn(EnvUtils.class).regex(onlyOurTargets)) {
      for (var entry : envVarToExpectedSysprop.entrySet()) {
        EnvUtils.init(true, Map.of(entry.getKey(), entry.getKey()), System.getProperties());
        assertEquals(
            "env var " + entry.getKey() + " should map to " + entry.getValue(),
            entry.getKey(),
            EnvUtils.getProperty(entry.getValue()));
      }
      assertEquals(
          "No deprecated-property warnings should be logged for these mappings",
          0,
          warnLog.getCount());
    }
  }

  @Test
  public void envToSyspropMappingsDoNotMapToDeprecatedSystemProperties() throws IOException {
    Properties envMappings = loadProperties("EnvToSyspropMappings.properties");
    Properties deprecatedMappings = loadProperties("DeprecatedSystemPropertyMappings.properties");
    Map<String, String> reverseDeprecatedMappings =
        deprecatedMappings.entrySet().stream()
            .collect(Collectors.toMap(e -> (String) e.getValue(), e -> (String) e.getKey()));

    for (String envVar : envMappings.stringPropertyNames()) {
      String sysProp = envMappings.getProperty(envVar);
      String newSysProp = reverseDeprecatedMappings.get(sysProp);
      if (newSysProp != null) {
        fail(
            "expected <"
                + sysProp
                + "> "
                + "mapped from <"
                + envVar
                + "> to not be deprecated, "
                + "but it was replaced by <"
                + newSysProp
                + ">");
      }
    }
  }

  private static Properties loadProperties(String resourceName) throws IOException {
    Properties properties = new Properties();
    try (var resource =
        new InputStreamReader(
            EnvUtils.class.getClassLoader().getResourceAsStream(resourceName),
            StandardCharsets.UTF_8)) {
      properties.load(resource);
    }
    return properties;
  }
}

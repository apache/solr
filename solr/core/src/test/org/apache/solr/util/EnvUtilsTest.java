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

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnvUtilsTest extends SolrTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    // Make a map of some common Solr environment variables for testing, and initialize EnvUtils
    EnvUtils.setEnvs(
        Map.of(
            "SOLR_HOME", "/home/solr",
            "SOLR_PORT", "8983",
            "SOLR_HOST", "localhost",
            "SOLR_LOG_LEVEL", "INFO",
            "SOLR_BOOLEAN", "true",
            "SOLR_LONG", "1234567890",
            "SOLR_COMMASEP", "one,two, three",
            "SOLR_JSON_LIST", "[\"one\", \"two\", \"three\"]",
            "SOLR_ALWAYS_ON_TRACE_ID", "true",
            "SOLR_STR_WITH_NEWLINE", "foo\nbar,baz"));
    EnvUtils.init(true);
  }

  @Test
  public void testGetEnv() {
    assertEquals("INFO", EnvUtils.getEnv("SOLR_LOG_LEVEL"));

    assertNull(EnvUtils.getEnv("SOLR_NONEXIST"));
    assertEquals("myString", EnvUtils.getEnv("SOLR_NONEXIST", "myString"));

    assertTrue(EnvUtils.getEnvAsBool("SOLR_BOOLEAN"));
    assertFalse(EnvUtils.getEnvAsBool("SOLR_BOOLEAN_NONEXIST", false));

    assertEquals("1234567890", EnvUtils.getEnv("SOLR_LONG"));
    assertEquals(1234567890L, EnvUtils.getEnvAsLong("SOLR_LONG"));
    assertEquals(987L, EnvUtils.getEnvAsLong("SOLR_LONG_NONEXIST", 987L));

    assertEquals("one,two, three", EnvUtils.getEnv("SOLR_COMMASEP"));
    assertEquals(List.of("one", "two", "three"), EnvUtils.getEnvAsList("SOLR_COMMASEP"));
    assertEquals(List.of("one", "two", "three"), EnvUtils.getEnvAsList("SOLR_JSON_LIST"));
    assertEquals(List.of("fallback"), EnvUtils.getEnvAsList("SOLR_MISSING", List.of("fallback")));
    assertEquals(List.of("foo\nbar", "baz"), EnvUtils.getEnvAsList("SOLR_STR_WITH_NEWLINE"));
  }

  @Test
  public void testGetProp() {
    assertEquals("INFO", EnvUtils.getProp("solr.log.level"));

    assertNull(EnvUtils.getProp("solr.nonexist"));
    assertEquals("myString", EnvUtils.getProp("solr.nonexist", "myString"));

    assertTrue(EnvUtils.getPropAsBool("solr.boolean"));
    assertFalse(EnvUtils.getPropAsBool("solr.boolean.nonexist", false));

    assertEquals("1234567890", EnvUtils.getProp("solr.long"));
    assertEquals(Long.valueOf(1234567890L), EnvUtils.getPropAsLong("solr.long"));
    assertEquals(Long.valueOf(987L), EnvUtils.getPropAsLong("solr.long.nonexist", 987L));

    assertEquals("one,two, three", EnvUtils.getProp("solr.commasep"));
    assertEquals(List.of("one", "two", "three"), EnvUtils.getPropAsList("solr.commasep"));
    assertEquals(List.of("one", "two", "three"), EnvUtils.getPropAsList("solr.json.list"));
    assertEquals(List.of("fallback"), EnvUtils.getPropAsList("SOLR_MISSING", List.of("fallback")));
  }

  @Test
  public void getPropWithCamelCase() {
    assertEquals("INFO", EnvUtils.getProp("solr.logLevel"));
    assertEquals("INFO", EnvUtils.getProp("solr.LogLevel"));
    assertEquals(Long.valueOf(1234567890L), EnvUtils.getPropAsLong("solrLong"));
    assertEquals(Boolean.TRUE, EnvUtils.getPropAsBool("solr.alwaysOnTraceId"));
    assertEquals(Boolean.TRUE, EnvUtils.getPropAsBool("solr.always.on.trace.id"));
  }

  @Test
  public void testEnvsWithCustomKeyNameMappings() {
    // These have different names than the environment variables
    assertEquals(EnvUtils.getEnv("SOLR_HOME"), EnvUtils.getProp("solr.solr.home"));
    assertEquals(EnvUtils.getEnv("SOLR_PORT"), EnvUtils.getProp("jetty.port"));
    assertEquals(EnvUtils.getEnv("SOLR_HOST"), EnvUtils.getProp("host"));
    assertEquals(EnvUtils.getEnv("SOLR_LOGS_DIR"), EnvUtils.getProp("solr.log.dir"));
  }

  @Test
  public void testNotMapped() {
    assertFalse(EnvUtils.getProps().containsKey("solr.ssl.key.store.password"));
    assertFalse(EnvUtils.getProps().containsKey("gc.log.opts"));
  }

  @Test
  public void testOverwrite() {
    EnvUtils.setProp("solr.overwrite", "original");
    EnvUtils.setEnv("SOLR_OVERWRITE", "overwritten");
    EnvUtils.init(false);
    assertEquals("original", EnvUtils.getProp("solr.overwrite"));
    EnvUtils.init(true);
    assertEquals("overwritten", EnvUtils.getProp("solr.overwrite"));
  }
}

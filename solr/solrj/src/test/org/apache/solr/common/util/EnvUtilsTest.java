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

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnvUtilsTest extends SolrTestCase {

  private static final Map<String, String> ENV =
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
          "SOLR_STR_WITH_NEWLINE", "foo\nbar,baz");

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Make a map of some common Solr environment variables for testing, and initialize EnvUtils
    EnvUtils.init(true, ENV);
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
    assertEquals(Boolean.TRUE, EnvUtils.getPropertyAsBool("solr.alwaysOnTraceId"));
    assertEquals(Boolean.TRUE, EnvUtils.getPropertyAsBool("solr.always.on.trace.id"));
  }

  @Test
  public void testEnvsWithCustomKeyNameMappings() {
    // These have different names than the environment variables
    assertEquals(ENV.get("SOLR_HOME"), EnvUtils.getProperty("solr.solr.home"));
    assertEquals(ENV.get("SOLR_PORT"), EnvUtils.getProperty("jetty.port"));
    assertEquals(ENV.get("SOLR_HOST"), EnvUtils.getProperty("host"));
    assertEquals(ENV.get("SOLR_LOGS_DIR"), EnvUtils.getProperty("solr.log.dir"));
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
    EnvUtils.init(false, env2);
    assertEquals("original", EnvUtils.getProperty("solr.overwrite"));
    EnvUtils.init(true, env2);
    assertEquals("overwritten", EnvUtils.getProperty("solr.overwrite"));
  }
}

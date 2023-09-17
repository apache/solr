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
            "SOLR_COMMASEP", "one,two, three"));
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
    assertEquals(List.of("one", "two", "three"), EnvUtils.getEnvCommaSepAsList("SOLR_COMMASEP"));
  }

  @Test
  public void testGetProp() {
    assertEquals("INFO", EnvUtils.getProp("solr.log.level"));

    assertNull(EnvUtils.getProp("solr.nonexist"));
    assertEquals("myString", EnvUtils.getProp("solr.nonexist", "myString"));

    assertTrue(EnvUtils.getPropAsBool("solr.boolean"));
    assertFalse(EnvUtils.getPropAsBool("solr.boolean.nonexist", false));

    assertEquals("1234567890", EnvUtils.getProp("solr.long"));
    assertEquals(1234567890L, EnvUtils.getPropAsLong("solr.long"));
    assertEquals(987L, EnvUtils.getPropAsLong("solr.long.nonexist", 987L));

    assertEquals("one,two, three", EnvUtils.getProp("solr.commasep"));
    assertEquals(List.of("one", "two", "three"), EnvUtils.getPropCommaSepAsList("solr.commasep"));
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
    EnvUtils.setProp("solr.log.level", "WARN");
    EnvUtils.setEnvs(Map.of("SOLR_LOG_LEVEL", "DEBUG"));
    EnvUtils.init(false);
    assertEquals("WARN", EnvUtils.getEnv("SOLR_LOG_LEVEL"));
    EnvUtils.init(true);
    assertEquals("DEBUG", EnvUtils.getEnv("SOLR_LOG_LEVEL"));
  }
}

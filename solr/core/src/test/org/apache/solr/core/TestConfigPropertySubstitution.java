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
package org.apache.solr.core;

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.ConfigNode;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class specifically for testing system property substitution in Solr configuration files.
 * This class is isolated from other tests to minimize the global impact of setting
 * test-specific system properties.
 */
public class TestConfigPropertySubstitution extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Set the test properties specifically for property substitution testing
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    // Use a minimal configuration file that only contains the propTest element
    initCore("solrconfig-test-properties.xml", "schema.xml");
  }

  @Test
  public void testJavaPropertySubstitution() {
    // Test that system property substitution works correctly in configuration files
    // These property values are set in beforeClass()

    String s = solrConfig.get("propTest").txt();
    assertEquals("prefix-proptwo-suffix", s);

    s = solrConfig.get("propTest").attr("attr1", "default");
    assertEquals("propone-${literal}", s);

    s = solrConfig.get("propTest").attr("attr2", "default");
    assertEquals("default-from-config", s);

    assertEquals(
        "prefix-proptwo-suffix",
        solrConfig.get("propTest", it -> "default-from-config".equals(it.attr("attr2"))).txt());

    List<ConfigNode> nl = solrConfig.root.getAll("propTest");
    assertEquals(1, nl.size());
    assertEquals("prefix-proptwo-suffix", nl.get(0).txt());

    assertEquals("prefix-proptwo-suffix", solrConfig.get("propTest").txt());
  }
}

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
package org.apache.solr;

import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test for Loading a custom core properties file referenced from the standard core.properties file.
 */
public class TestCustomCoreProperties extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  // TODO these properties files don't work with configsets

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path homeDir = createTempDir();

    Path collDir = homeDir.resolve("collection1");
    Path confDir = collDir.resolve("conf");

    Files.createDirectories(confDir);

    Path srcDir = TEST_HOME().resolve("collection1").resolve("conf");
    Files.copy(srcDir.resolve("schema-tiny.xml"), confDir.resolve("schema.xml"));
    Files.copy(srcDir.resolve("solrconfig-coreproperties.xml"), confDir.resolve("solrconfig.xml"));
    Files.copy(
        srcDir.resolve("solrconfig.snippet.randomindexconfig.xml"),
        confDir.resolve("solrconfig.snippet.randomindexconfig.xml"));

    Properties p = new Properties();
    p.setProperty("foo.foo1", "f1");
    p.setProperty("foo.foo2", "f2");
    var coreCustomProperties = confDir.resolve("core_custom_properties.properties");
    try (Writer fos = Files.newBufferedWriter(coreCustomProperties, StandardCharsets.UTF_8)) {
      p.store(fos, null);
    }

    Properties coreProperties = new Properties();
    coreProperties.setProperty(CoreDescriptor.CORE_PROPERTIES, coreCustomProperties.toString());
    try (Writer fos =
        Files.newBufferedWriter(collDir.resolve("core.properties"), StandardCharsets.UTF_8)) {
      coreProperties.store(fos, null);
    }

    solrTestRule.startSolr(homeDir, new Properties(), JettyConfig.builder().build());
  }

  @Test
  public void testSimple() throws Exception {
    SolrParams params =
        params(
            "q", "*:*",
            "echoParams", "all");
    QueryResponse res = solrTestRule.getSolrClient("collection1").query(params);
    assertEquals(0, res.getResults().getNumFound());

    NamedList<?> echoedParams = (NamedList<?>) res.getHeader().get("params");
    assertEquals("f1", echoedParams.get("p1"));
    assertEquals("f2", echoedParams.get("p2"));
  }
}

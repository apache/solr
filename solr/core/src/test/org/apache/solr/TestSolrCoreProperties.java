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
import org.junit.BeforeClass;

/**
 * Test for Loading core properties from a properties file
 *
 * @since solr 1.4
 */
public class TestSolrCoreProperties extends SolrJettyTestBase {

  // TODO these properties files don't work with configsets

  @BeforeClass
  public static void beforeTest() throws Exception {
    Path homeDir = createTempDir();

    Path collDir = homeDir.resolve("collection1");
    Path dataDir = collDir.resolve("data");
    Path confDir = collDir.resolve("conf");

    Files.createDirectories(homeDir);
    Files.createDirectories(collDir);
    Files.createDirectories(dataDir);
    Files.createDirectories(confDir);

    Files.copy(Path.of(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), homeDir.resolve("solr.xml"));
    String src_dir = TEST_HOME() + "/collection1/conf";
    Files.copy(Path.of(src_dir, "schema-tiny.xml"), confDir.resolve("schema.xml"));
    Files.copy(
        Path.of(src_dir, "solrconfig-coreproperties.xml"), confDir.resolve("solrconfig.xml"));
    Files.copy(
        Path.of(src_dir, "solrconfig.snippet.randomindexconfig.xml"),
        confDir.resolve("solrconfig.snippet.randomindexconfig.xml"));

    Properties p = new Properties();
    p.setProperty("foo.foo1", "f1");
    p.setProperty("foo.foo2", "f2");
    try (Writer fos =
        Files.newBufferedWriter(confDir.resolve("solrcore.properties"), StandardCharsets.UTF_8)) {
      p.store(fos, null);
    }

    Files.createFile(collDir.resolve("core.properties"));

    Properties nodeProperties = new Properties();
    // this sets the property for jetty starting SolrDispatchFilter
    if (System.getProperty("solr.data.dir") == null) {
      nodeProperties.setProperty("solr.data.dir", createTempDir().toFile().getCanonicalPath());
    }

    solrClientTestRule.startSolr(homeDir, nodeProperties, buildJettyConfig("/solr"));

    // createJetty(homeDir.getAbsolutePath(), null, null);
  }

  public void testSimple() throws Exception {
    SolrParams params =
        params(
            "q", "*:*",
            "echoParams", "all");
    QueryResponse res = getSolrClient().query(params);
    assertEquals(0, res.getResults().getNumFound());

    NamedList<?> echoedParams = (NamedList<?>) res.getHeader().get("params");
    assertEquals("f1", echoedParams.get("p1"));
    assertEquals("f2", echoedParams.get("p2"));
  }
}

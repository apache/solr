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

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrTestCaseJ4Test extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Create a temporary directory that holds a core NOT named "collection1". Use the smallest
    // configuration sets we can, so we don't copy that much junk around.
    Path tmpSolrHome = createTempDir();

    Path subHome = tmpSolrHome.resolve("core0/conf");
    Files.createDirectories(subHome);
    Path top = SolrTestCaseJ4.TEST_HOME().resolve("collection1/conf");
    Files.copy(top.resolve("schema-tiny.xml"), subHome.resolve("schema-tiny.xml"));
    Files.copy(top.resolve("solrconfig-minimal.xml"), subHome.resolve("solrconfig-minimal.xml"));
    Files.copy(
        top.resolve("solrconfig.snippet.randomindexconfig.xml"),
        subHome.resolve("solrconfig.snippet.randomindexconfig.xml"));

    PathUtils.copyDirectory(tmpSolrHome.resolve("core0"), tmpSolrHome.resolve("core1"));
    // Core discovery will default to the name of the dir the core.properties file is in. So if
    // everything else is OK as defaults, just the _presence_ of this file is sufficient.
    PathUtils.touch(tmpSolrHome.resolve("core0/core.properties"));
    PathUtils.touch(tmpSolrHome.resolve("core1/core.properties"));

    Files.copy(getFile("solr/solr.xml"), tmpSolrHome.resolve("solr.xml"));

    initCore("solrconfig-minimal.xml", "schema-tiny.xml", tmpSolrHome, "core1");
  }

  @AfterClass
  public static void AfterClass() {}

  @Test
  public void testCorrectCore() {
    assertEquals("should be core1", "core1", h.getCore().getName());
  }

  @Test
  public void testParams() {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    assertEquals(params.toString(), params().toString());

    params.add("q", "*:*");
    assertEquals(params.toString(), params("q", "*:*").toString());

    params.add("rows", "42");
    assertEquals(params.toString(), params("q", "*:*", "rows", "42").toString());

    expectThrows(
        RuntimeException.class,
        () -> {
          params("parameterWithoutValue");
        });

    expectThrows(
        RuntimeException.class,
        () -> {
          params("q", "*:*", "rows", "42", "parameterWithoutValue");
        });
  }
}

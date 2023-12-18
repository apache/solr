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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class TestConfigSets extends SolrTestCaseJ4 {

  @Rule public TestRule testRule = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  public static String solrxml =
      "<solr><str name=\"configSetBaseDir\">${configsets:configsets}</str></solr>";

  public CoreContainer setupContainer(String configSetsBaseDir) {
    Path testDirectory = createTempDir();

    System.setProperty("configsets", configSetsBaseDir);

    CoreContainer container = new CoreContainer(SolrXmlConfig.fromString(testDirectory, solrxml));
    container.load();

    return container;
  }

  @Test
  public void testDefaultConfigSetBasePathResolution() {
    Path solrHome = Paths.get("/path/to/solr/home");

    NodeConfig config =
        SolrXmlConfig.fromString(
            solrHome, "<solr><str name=\"configSetBaseDir\">configsets</str></solr>");
    MatcherAssert.assertThat(
        config.getConfigSetBaseDirectory().toAbsolutePath(),
        is(Paths.get("/path/to/solr/home/configsets").toAbsolutePath()));

    NodeConfig absConfig =
        SolrXmlConfig.fromString(
            solrHome, "<solr><str name=\"configSetBaseDir\">/path/to/configsets</str></solr>");
    MatcherAssert.assertThat(
        absConfig.getConfigSetBaseDirectory().toAbsolutePath(),
        is(Paths.get("/path/to/configsets").toAbsolutePath()));
  }

  @Test
  public void testConfigSetServiceFindsConfigSets() {
    CoreContainer container = null;
    try {
      container = setupContainer(TEST_PATH().resolve("configsets").toString());
      Path solrHome = Paths.get(container.getSolrHome());

      SolrCore core1 = container.create("core1", Map.of("configSet", "configset-2"));
      MatcherAssert.assertThat(core1.getCoreDescriptor().getName(), is("core1"));
      MatcherAssert.assertThat(
          Paths.get(core1.getDataDir()).toString(),
          is(solrHome.resolve("core1").resolve("data").toString()));
    } finally {
      if (container != null) container.shutdown();
    }
  }

  @Test
  public void testNonExistentConfigSetThrowsException() {
    final CoreContainer container = setupContainer(getFile("solr/configsets").getAbsolutePath());
    try {
      Exception thrown =
          expectThrows(
              Exception.class,
              "Expected core creation to fail",
              () -> {
                container.create("core1", Map.of("configSet", "nonexistent"));
              });
      Throwable wrappedException = getWrappedException(thrown);
      MatcherAssert.assertThat(wrappedException.getMessage(), containsString("nonexistent"));
    } finally {
      if (container != null) container.shutdown();
    }
  }

  @Test
  public void testConfigSetOnCoreReload() throws IOException {
    Path testDirectory = createTempDir("core-reload");
    Path configSetsDir = testDirectory.resolve("configsets");

    PathUtils.copyDirectory(getFile("solr/configsets").toPath(), configSetsDir);

    String csd = configSetsDir.toAbsolutePath().toString();
    System.setProperty("configsets", csd);

    CoreContainer container = new CoreContainer(SolrXmlConfig.fromString(testDirectory, solrxml));
    container.load();

    // We initially don't have a /dump handler defined
    SolrCore core = container.create("core1", Map.of("configSet", "configset-2"));
    MatcherAssert.assertThat(
        "No /dump handler should be defined in the initial configuration",
        core.getRequestHandler("/dump"),
        is(nullValue()));

    // Now copy in a config with a /dump handler and reload
    Files.copy(
        getFile("solr/collection1/conf/solrconfig-withgethandler.xml").toPath(),
        configSetsDir.resolve("configset-2/conf").resolve("solrconfig.xml"),
        StandardCopyOption.REPLACE_EXISTING);
    container.reload("core1");

    core = container.getCore("core1");
    MatcherAssert.assertThat(
        "A /dump handler should be defined in the reloaded configuration",
        core.getRequestHandler("/dump"),
        is(notNullValue()));
    core.close();

    container.shutdown();
  }
}

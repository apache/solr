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

import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCorePropertiesReload extends SolrTestCaseJ4 {

  private static Path solrHomeDirectory;
  private static CoreContainer coreContainer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrHomeDirectory = createTempDir();
    PathUtils.copyDirectory(TEST_HOME(), solrHomeDirectory);

    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    Properties props = new Properties();
    props.setProperty("test", "Before reload");
    writeCustomProperties(props);

    writeCoreProperties();
    String solrXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" + "<solr></solr>";

    coreContainer = createCoreContainer(solrHomeDirectory, solrXml);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (coreContainer != null) {
      coreContainer.shutdown();
      coreContainer = null;
    }
  }

  @Test
  public void testPropertiesReload() throws Exception {
    try (SolrCore core = coreContainer.getCore("collection1")) {
      assertNotNull("Core collection1 should exist", core);
      CoreDescriptor coreDescriptor = core.getCoreDescriptor();
      String testProp = coreDescriptor.getCoreProperty("test", null);
      assertEquals("Before reload", testProp);
    }

    Properties props = new Properties();
    props.setProperty("test", "After reload");
    writeCustomProperties(props);

    coreContainer.reload("collection1");

    try (SolrCore core = coreContainer.getCore("collection1")) {
      CoreDescriptor coreDescriptor = core.getCoreDescriptor();
      String testProp = coreDescriptor.getCoreProperty("test", null);
      assertEquals("After reload", testProp);
    }
  }

  private static void writeCoreProperties() throws Exception {
    Path coreDir = solrHomeDirectory.resolve("collection1");
    Path propFile = coreDir.resolve("core.properties");

    Files.createDirectories(coreDir);
    Properties coreProps = new Properties();
    coreProps.setProperty(CoreDescriptor.CORE_NAME, "collection1");
    coreProps.setProperty(CoreDescriptor.CORE_CONFIG, "solrconfig.xml");
    coreProps.setProperty(CoreDescriptor.CORE_SCHEMA, "schema.xml");
    coreProps.setProperty(CoreDescriptor.CORE_PROPERTIES, "custom.properties");

    try (Writer out = Files.newBufferedWriter(propFile, StandardCharsets.UTF_8)) {
      coreProps.store(out, null);
    }
  }

  private static void writeCustomProperties(Properties props) throws Exception {
    Path coreDir = solrHomeDirectory.resolve("collection1");
    Path propFile = coreDir.resolve("custom.properties");

    Files.createDirectories(coreDir);

    try (Writer out = Files.newBufferedWriter(propFile, StandardCharsets.UTF_8)) {
      props.store(out, null);
    }
  }
}

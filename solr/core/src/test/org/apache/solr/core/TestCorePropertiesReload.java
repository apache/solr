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

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class TestCorePropertiesReload extends SolrTestCaseJ4 {

  private final Path solrHomeDirectory = createTempDir();
  private CoreContainer coreContainer;

  public void setMeUp() throws Exception {
    PathUtils.copyDirectory(TEST_HOME(), solrHomeDirectory);

    // Set system properties that the test config expects
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    // Write custom properties to custom.properties
    Properties props = new Properties();
    props.setProperty("test", "Before reload");
    writeCustomProperties(props);

    // Write core.properties that references the custom.properties file
    writeCoreProperties();

    // Create a minimal solr.xml
    String solrXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" + "<solr></solr>";

    // Create core container with CorePropertiesLocator
    coreContainer = createCoreContainer(solrHomeDirectory, solrXml);
  }

  @Test
  public void testPropertiesReload() throws Exception {
    setMeUp();
    try (SolrCore core = coreContainer.getCore("collection1")) {
      assertNotNull("Core collection1 should exist", core);
      CoreDescriptor coreDescriptor = core.getCoreDescriptor();
      String testProp = coreDescriptor.getCoreProperty("test", null);
      assertEquals("Before reload", testProp);
    }

    // Re-write the custom properties file
    Properties props = new Properties();
    props.setProperty("test", "After reload");
    writeCustomProperties(props);

    // Reload the core
    coreContainer.reload("collection1");

    try (SolrCore core = coreContainer.getCore("collection1")) {
      CoreDescriptor coreDescriptor = core.getCoreDescriptor();
      String testProp = coreDescriptor.getCoreProperty("test", null);
      assertEquals("After reload", testProp);
    }
  }

  private void writeCoreProperties() throws Exception {
    Path coreDir = solrHomeDirectory.resolve("collection1");
    Path propFile = coreDir.resolve("core.properties");

    // Ensure the directory exists
    Files.createDirectories(coreDir);

    // Write core.properties with required properties and reference to the custom properties file
    Properties coreProps = new Properties();
    coreProps.setProperty(CoreDescriptor.CORE_NAME, "collection1");
    coreProps.setProperty(CoreDescriptor.CORE_CONFIG, "solrconfig.xml");
    coreProps.setProperty(CoreDescriptor.CORE_SCHEMA, "schema.xml");
    coreProps.setProperty(CoreDescriptor.CORE_PROPERTIES, "custom.properties");

    try (Writer out =
        new BufferedWriter(
            new OutputStreamWriter(Files.newOutputStream(propFile), StandardCharsets.UTF_8))) {
      coreProps.store(out, "Core Properties");
    }
  }

  private void writeCustomProperties(Properties props) throws Exception {
    Path coreDir = solrHomeDirectory.resolve("collection1");
    Path propFile = coreDir.resolve("custom.properties");

    // Ensure the directory exists
    Files.createDirectories(coreDir);

    try (Writer out =
        new BufferedWriter(
            new OutputStreamWriter(Files.newOutputStream(propFile), StandardCharsets.UTF_8))) {
      props.store(out, "Custom Properties");
    }
  }
}

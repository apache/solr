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

package org.apache.solr.client.ref_guide_examples;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Examples showing how to manipulate configsets in ZK.
 *
 * <p>Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference
 * Guide.
 */
public class ZkConfigFilesTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setUpCluster() throws Exception {
    configureCluster(1).configure();
  }

  private static ConfigSetService getConfigSetService() {
    return cluster.getOpenOverseer().getCoreContainer().getConfigSetService();
  }

  @Before
  public void clearConfigsBefore() throws Exception {
    clearConfigs();
  }

  @After
  public void clearConfigsAfter() throws Exception {
    clearConfigs();
  }

  private void clearConfigs() throws Exception {
    List<String> configs = getConfigSetService().listConfigs();
    for (String config : configs) {
      getConfigSetService().deleteConfig(config);
    }
  }

  @Test
  public void testCanUploadConfigToZk() throws Exception {
    final String localConfigSetDirectory =
        new File(ExternalPaths.TECHPRODUCTS_CONFIGSET).getAbsolutePath();

    assertConfigsContainOnly();

    // tag::zk-configset-upload[]
    getConfigSetService().uploadConfig("nameForConfigset", Paths.get(localConfigSetDirectory));
    // end::zk-configset-upload[]

    assertConfigsContainOnly("nameForConfigset");
  }

  private void assertConfigsContainOnly(String... expectedConfigs) throws Exception {
    final int expectedSize = expectedConfigs.length;

    List<String> actualConfigs = getConfigSetService().listConfigs();

    assertEquals(expectedSize, actualConfigs.size());
    for (String expectedConfig : expectedConfigs) {
      assertTrue(
          "Expected ZK to contain " + expectedConfig + ", but it didn't.  Actual configs: ",
          actualConfigs.contains(expectedConfig));
    }
  }
}

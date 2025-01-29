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
package org.apache.solr.client.solrj.embedded;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.NodeConfig;
import org.junit.Test;

public class TestEmbeddedSolrServerConstructors extends SolrTestCaseJ4 {

  @Test
  @SuppressWarnings({"try"})
  public void testPathConstructor() throws IOException {
    try (EmbeddedSolrServer server = new EmbeddedSolrServer(TEST_PATH(), "collection1")) {}
  }

  @Test
  public void testNodeConfigConstructor() throws Exception {
    Path path = createTempDir();

    NodeConfig config =
        new NodeConfig.NodeConfigBuilder("testnode", path)
            .setConfigSetBaseDirectory(TEST_PATH().resolve("configsets").toString())
            .build();

    try (EmbeddedSolrServer server = new EmbeddedSolrServer(config, "newcore")) {

      CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
      createRequest.setCoreName("newcore");
      createRequest.setConfigSet("minimal");
      server.request(createRequest);

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("articleid", "test");
      server.add("newcore", doc);
      server.commit();

      assertEquals(1, server.query(new SolrQuery("*:*")).getResults().getNumFound());
      assertEquals(1, server.query("newcore", new SolrQuery("*:*")).getResults().getNumFound());
    }
  }

  @SuppressWarnings("removal")
  @Test
  public void testPathConstructorZipFS() throws Exception {
    assumeTrue(
        "Test only works without Security Manager due to SecurityConfHandlerLocal" +
                "missing permission to read /1/2/3/4/security.json file",
        System.getSecurityManager() == null);

    Path dataDir = createTempDir("data-dir");
    Path archive = createTempFile("configset", ".zip");
    Files.delete(archive);

    // When :
    // Prepare a zip archive which contains
    // the configset files as shown below:
    //
    // configset.zip
    // └── 1
    //     └── 2
    //         └── 3
    //             └── 4
    //                 ├── data
    //                 │   └── core1
    //                 │       ├── conf
    //                 │       │   ├── managed-schema.xml
    //                 │       │   └── solrconfig.xml
    //                 │       └── core.properties
    //                 └── solr.xml

    var zipFs = FileSystems.newFileSystem(archive, Map.of("create", "true"));
    try (zipFs) {
      var destDir = zipFs.getPath("1", "2", "3", "4");
      var confDir = destDir.resolve("data/core1/conf");
      Files.createDirectories(confDir);
      Files.copy(TEST_PATH().resolve("solr.xml"), destDir.resolve("solr.xml"));
      PathUtils.copyDirectory(configset("zipfs"), confDir);

      // Need to make sure we circumvent any Solr attempts
      // to modify the archive - so to achieve that we:
      // - set a custom data dir,
      // - disable the update log,
      // - configure the rest manager in the solrconfig.xml with InMemoryStorageIO.
      Files.writeString(
          confDir.resolveSibling("core.properties"),
          String.join("\n", "solr.ulog.enable=false", "solr.data.dir=" + dataDir));
    }

    // Then :
    // EmbeddedSolrServer successfully loads the configset directly from the archive
    var configSetFs = FileSystems.newFileSystem(archive);
    try (configSetFs) {
      var server = new EmbeddedSolrServer(configSetFs.getPath("/1/2/3/4"), null);
      try (server) {
        assertEquals(List.of("core1"), server.getCoreContainer().getAllCoreNames());
      }
    }
  }
}

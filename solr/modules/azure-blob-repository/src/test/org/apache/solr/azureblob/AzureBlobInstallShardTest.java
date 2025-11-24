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
package org.apache.solr.azureblob;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class AzureBlobInstallShardTest extends AbstractAzureBlobClientTest {

  @Test
  public void testInstallShard() throws Exception {
    String shardPath = "install-shard-test/";

    client.createDirectory(shardPath);
    client.createDirectory(shardPath + "index/");
    client.createDirectory(shardPath + "conf/");

    pushContent(shardPath + "index/segments_1", "Shard index segments");
    pushContent(shardPath + "index/_0.cfs", "Shard index file");
    pushContent(shardPath + "conf/solrconfig.xml", "Shard configuration");
    pushContent(shardPath + "conf/schema.xml", "Shard schema");

    assertTrue("Shard directory should exist", client.pathExists(shardPath));
    assertTrue("Index directory should exist", client.pathExists(shardPath + "index/"));
    assertTrue("Conf directory should exist", client.pathExists(shardPath + "conf/"));
    assertTrue("Segments file should exist", client.pathExists(shardPath + "index/segments_1"));
    assertTrue("Index file should exist", client.pathExists(shardPath + "index/_0.cfs"));
    assertTrue("Config file should exist", client.pathExists(shardPath + "conf/solrconfig.xml"));
    assertTrue("Schema file should exist", client.pathExists(shardPath + "conf/schema.xml"));
  }

  @Test
  public void testInstallShardWithMultipleIndexFiles() throws Exception {
    String shardPath = "multi-index-shard-test/";
    String[] indexFiles = {"segments_1", "_0.cfs", "_0.cfe", "_0.si", "_1.cfs", "_1.cfe", "_1.si"};

    client.createDirectory(shardPath);
    client.createDirectory(shardPath + "index/");

    for (String indexFile : indexFiles) {
      pushContent(shardPath + "index/" + indexFile, "Index file content: " + indexFile);
    }

    for (String indexFile : indexFiles) {
      assertTrue(
          "Index file should exist: " + indexFile,
          client.pathExists(shardPath + "index/" + indexFile));
    }
  }

  @Test
  public void testInstallShardWithDataFiles() throws Exception {
    String shardPath = "data-shard-test/";
    String[] dataFiles = {
      "tlog.0000000000000000001", "tlog.0000000000000000002", "tlog.0000000000000000003"
    };

    client.createDirectory(shardPath);
    client.createDirectory(shardPath + "data/");

    for (String dataFile : dataFiles) {
      pushContent(shardPath + "data/" + dataFile, "Transaction log: " + dataFile);
    }

    for (String dataFile : dataFiles) {
      assertTrue(
          "Data file should exist: " + dataFile, client.pathExists(shardPath + "data/" + dataFile));
    }
  }

  @Test
  public void testInstallShardWithConfiguration() throws Exception {
    String shardPath = "config-shard-test/";
    String solrConfig =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
            + "<config>\n"
            + "  <luceneMatchVersion>LATEST</luceneMatchVersion>\n"
            + "  <directoryFactory class=\"solr.NRTCachingDirectoryFactory\"/>\n"
            + "</config>";

    String schema =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
            + "<schema name=\"test\" version=\"1.6\">\n"
            + "  <field name=\"id\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\" />\n"
            + "</schema>";

    client.createDirectory(shardPath);
    client.createDirectory(shardPath + "conf/");

    pushContent(shardPath + "conf/solrconfig.xml", solrConfig);
    pushContent(shardPath + "conf/schema.xml", schema);

    assertTrue("Solr config should exist", client.pathExists(shardPath + "conf/solrconfig.xml"));
    assertTrue("Schema should exist", client.pathExists(shardPath + "conf/schema.xml"));

    try (var input = client.pullStream(shardPath + "conf/solrconfig.xml")) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertTrue(
          "Solr config should contain expected content",
          readContent.contains("luceneMatchVersion"));
    }
  }

  @Test
  public void testInstallShardWithLargeIndex() throws Exception {
    String shardPath = "large-index-shard-test/";
    StringBuilder largeContent = new StringBuilder();
    for (int i = 0; i < 50000; i++) {
      largeContent.append("Index data line ").append(i).append("\n");
    }

    client.createDirectory(shardPath);
    client.createDirectory(shardPath + "index/");

    pushContent(shardPath + "index/large-index.cfs", largeContent.toString());

    assertTrue(
        "Large index file should exist", client.pathExists(shardPath + "index/large-index.cfs"));
    assertEquals(
        "Large index file length should match",
        largeContent.length(),
        client.length(shardPath + "index/large-index.cfs"));
  }

  @Test
  public void testInstallShardWithBinaryIndex() throws Exception {
    String shardPath = "binary-index-shard-test/";
    byte[] binaryData = new byte[2048];
    for (int i = 0; i < binaryData.length; i++) {
      binaryData[i] = (byte) (i % 256);
    }

    client.createDirectory(shardPath);
    client.createDirectory(shardPath + "index/");

    pushContent(shardPath + "index/binary-index.cfs", binaryData);

    assertTrue(
        "Binary index file should exist", client.pathExists(shardPath + "index/binary-index.cfs"));
    assertEquals(
        "Binary index file length should match",
        binaryData.length,
        client.length(shardPath + "index/binary-index.cfs"));
  }

  @Test
  public void testInstallShardWithNestedStructure() throws Exception {
    String shardPath = "nested-shard-test/";

    client.createDirectory(shardPath);
    client.createDirectory(shardPath + "index/");
    client.createDirectory(shardPath + "conf/");
    client.createDirectory(shardPath + "data/");
    client.createDirectory(shardPath + "logs/");

    pushContent(shardPath + "index/segments_1", "Segments file");
    pushContent(shardPath + "conf/solrconfig.xml", "Config file");
    pushContent(shardPath + "data/tlog.1", "Transaction log");
    pushContent(shardPath + "logs/solr.log", "Log file");

    assertTrue("Root shard should exist", client.pathExists(shardPath));
    assertTrue("Index directory should exist", client.pathExists(shardPath + "index/"));
    assertTrue("Conf directory should exist", client.pathExists(shardPath + "conf/"));
    assertTrue("Data directory should exist", client.pathExists(shardPath + "data/"));
    assertTrue("Logs directory should exist", client.pathExists(shardPath + "logs/"));
    assertTrue("Segments file should exist", client.pathExists(shardPath + "index/segments_1"));
    assertTrue("Config file should exist", client.pathExists(shardPath + "conf/solrconfig.xml"));
    assertTrue("Transaction log should exist", client.pathExists(shardPath + "data/tlog.1"));
    assertTrue("Log file should exist", client.pathExists(shardPath + "logs/solr.log"));
  }

  @Test
  public void testInstallShardWithMetadata() throws Exception {
    String shardPath = "metadata-shard-test/";
    String metadata =
        "{\n"
            + "  \"shardId\": \"shard1\",\n"
            + "  \"coreName\": \"test-core\",\n"
            + "  \"version\": \"1.0\",\n"
            + "  \"timestamp\": \"2023-01-01T00:00:00Z\"\n"
            + "}";

    client.createDirectory(shardPath);

    pushContent(shardPath + "shard-metadata.json", metadata);
    pushContent(shardPath + "index/segments_1", "Index segments");

    assertTrue("Metadata file should exist", client.pathExists(shardPath + "shard-metadata.json"));
    assertTrue("Index file should exist", client.pathExists(shardPath + "index/segments_1"));

    try (var input = client.pullStream(shardPath + "shard-metadata.json")) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertTrue("Metadata should contain shard ID", readContent.contains("shard1"));
      assertTrue("Metadata should contain core name", readContent.contains("test-core"));
    }
  }

  @Test
  public void testInstallShardCleanup() throws Exception {
    String shardPath = "cleanup-shard-test/";

    client.createDirectory(shardPath);
    client.createDirectory(shardPath + "index/");
    client.createDirectory(shardPath + "conf/");

    pushContent(shardPath + "index/segments_1", "Index segments");
    pushContent(shardPath + "conf/solrconfig.xml", "Config file");

    assertTrue("Shard should exist", client.pathExists(shardPath));

    client.deleteDirectory(shardPath);

    assertFalse("Shard should not exist after cleanup", client.pathExists(shardPath));
    assertFalse(
        "Index directory should not exist after cleanup", client.pathExists(shardPath + "index/"));
    assertFalse(
        "Conf directory should not exist after cleanup", client.pathExists(shardPath + "conf/"));
  }
}

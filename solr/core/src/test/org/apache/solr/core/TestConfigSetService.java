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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkConfigSetService;
import org.apache.solr.cloud.ZkTestServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestConfigSetService extends SolrTestCaseJ4 {

  private final ConfigSetService configSetService;
  private static ZkTestServer zkServer;

  @BeforeClass
  public static void startZkServer() throws Exception {
    zkServer = new ZkTestServer(createTempDir("zkData"));
    zkServer.run();
  }

  @AfterClass
  public static void shutdownZkServer() throws IOException, InterruptedException {
    if (null != zkServer) {
      zkServer.shutdown();
    }
    zkServer = null;
  }

  public TestConfigSetService(Supplier<ConfigSetService> configSetService) {
    this.configSetService = configSetService.get();
  }

  @ParametersFactory
  public static Iterable<Supplier<?>[]> parameters() {
    return Arrays.asList(
        new Supplier<?>[][] {
          {() -> new ZkConfigSetService(zkServer.getZkClient())},
          {() -> new FileSystemConfigSetService(createTempDir("configsets"))}
        });
  }

  @Test
  public void testConfigSetServiceOperations() throws IOException {
    final String configName = "testconfig";
    byte[] testdata = "test data".getBytes(StandardCharsets.UTF_8);

    Path configDir = createTempDir("testconfig");
    Files.createFile(configDir.resolve("solrconfig.xml"));
    Files.write(configDir.resolve("file1"), testdata);
    Files.createFile(configDir.resolve("file2"));
    Files.createDirectory(configDir.resolve("subdir"));
    Files.createFile(configDir.resolve("subdir").resolve("file3"));

    configSetService.uploadConfig(configName, configDir);

    assertTrue(configSetService.checkConfigExists(configName));
    assertFalse(configSetService.checkConfigExists("dummyConfig"));

    byte[] data = "file3 data".getBytes(StandardCharsets.UTF_8);
    configSetService.uploadFileToConfig(configName, "subdir/file3", data, true);
    assertArrayEquals(configSetService.downloadFileFromConfig(configName, "subdir/file3"), data);

    data = "file4 data".getBytes(StandardCharsets.UTF_8);
    configSetService.uploadFileToConfig(configName, "subdir/file4", data, true);
    assertArrayEquals(configSetService.downloadFileFromConfig(configName, "subdir/file4"), data);

    Map<String, Object> metadata = configSetService.getConfigMetadata(configName);
    assertTrue(metadata.isEmpty());

    configSetService.setConfigMetadata(configName, Collections.singletonMap("trusted", true));
    metadata = configSetService.getConfigMetadata(configName);
    assertTrue(metadata.containsKey("trusted"));

    configSetService.setConfigMetadata(configName, Collections.singletonMap("foo", true));
    assertFalse(configSetService.getConfigMetadata(configName).containsKey("trusted"));
    assertTrue(configSetService.getConfigMetadata(configName).containsKey("foo"));

    List<String> configFiles = configSetService.getAllConfigFiles(configName);
    assertEquals(
        configFiles,
        List.of("file1", "file2", "solrconfig.xml", "subdir/", "subdir/file3", "subdir/file4"));

    List<String> configs = configSetService.listConfigs();
    assertEquals(configs, List.of("testconfig"));

    configSetService.copyConfig(configName, "testconfig.AUTOCREATED");
    List<String> copiedConfigFiles = configSetService.getAllConfigFiles("testconfig.AUTOCREATED");
    assertEquals(configFiles, copiedConfigFiles);

    assertEquals(2, configSetService.listConfigs().size());

    configSetService.deleteConfig("testconfig.AUTOCREATED");
    assertFalse(configSetService.checkConfigExists("testconfig.AUTOCREATED"));

    configSetService.deleteConfig(configName);
    assertFalse(configSetService.checkConfigExists(configName));
  }
}

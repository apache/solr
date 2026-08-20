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

package org.apache.solr.cli;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.SecurityJson;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateToolTest extends SolrCloudTestCase {

  private static final String collectionName = "testCreateCollectionWithBasicAuth";

  @BeforeClass
  public static void setupClusterWithSecurityEnabled() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SecurityJson.SIMPLE)
        .configure();
  }

  @Test
  public void testCreateCollectionWithBasicAuth() throws Exception {

    String[] args = {
      "create",
      "-c",
      collectionName,
      "-n",
      "cloud-minimal",
      "-z",
      cluster.getZkClient().getZkServerAddress(),
      "--credentials",
      SecurityJson.USER_PASS,
      "--verbose"
    };

    assertEquals(0, CLITestHelper.runTool(args, CreateTool.class));
  }

  @Test
  public void testCreateCollectionUploadsNewConfigSet() throws Exception {
    String[] args = {
      "create",
      "-c",
      "testCreateCollectionUploadsNewConfigSet",
      "-d",
      configset("cloud-minimal").toString(),
      "-n",
      "cloud-minimal-uploaded",
      "-z",
      cluster.getZkClient().getZkServerAddress(),
      "--credentials",
      SecurityJson.USER_PASS,
      "--verbose"
    };

    assertEquals(0, CLITestHelper.runTool(args, CreateTool.class));
    assertTrue(cluster.getZkClient().exists("/configs/cloud-minimal-uploaded"));
  }

  @Test
  public void testZipConfigSetSkipsHiddenFilesAndIncludesDirectoryEntries() throws Exception {
    Path confDir = createTempDir("zipConfigSetTest");
    Files.writeString(confDir.resolve("solrconfig.xml"), "<config/>");
    Files.writeString(confDir.resolve(".hidden-file"), "should not be zipped");
    Path langDir = Files.createDirectory(confDir.resolve("lang"));
    Files.writeString(langDir.resolve("stopwords.txt"), "the\na\n");
    Path hiddenDir = Files.createDirectory(confDir.resolve(".hiddenDir"));
    Files.writeString(hiddenDir.resolve("nope.txt"), "should not be zipped either");

    byte[] zipBytes = CreateTool.zipConfigSet(confDir);

    Set<String> entryNames = new HashSet<>();
    try (ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zipIn.getNextEntry()) != null) {
        entryNames.add(entry.getName());
      }
    }

    assertTrue(entryNames.contains("solrconfig.xml"));
    assertTrue(entryNames.contains("lang/"));
    assertTrue(entryNames.contains("lang/stopwords.txt"));
    assertFalse(entryNames.contains(".hidden-file"));
    assertTrue(entryNames.stream().noneMatch(name -> name.startsWith(".hiddenDir")));
  }

  @Test
  public void testZipConfigSetRejectsForbiddenFileType() throws Exception {
    Path confDir = createTempDir("zipConfigSetForbiddenTest");
    Files.writeString(confDir.resolve("evil.jar"), "not really a jar");

    IOException thrown = expectThrows(IOException.class, () -> CreateTool.zipConfigSet(confDir));
    assertTrue(thrown.getMessage().contains("forbidden"));
  }
}

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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.ZkConfigSetService;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.DigestZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider;
import org.apache.solr.common.cloud.VMParamsZkCredentialsInjector;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ZLibCompressor;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: This test would be a lot faster if it used a solrhome with fewer config
// files - there are a lot of them to upload
public class ZkSubcommandsTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected ZkTestServer zkServer;

  protected Path zkDir;

  private String solrHome;

  private SolrZkClient zkClient;

  private PrintStream originalSystemOut;

  protected static final Path SOLR_HOME = SolrTestCaseJ4.TEST_HOME();

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("solrcloud.skip.autorecovery");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (log.isInfoEnabled()) {
      log.info("####SETUP_START {}", getTestName());
    }

    Path exampleHome = legacyExampleCollection1SolrHome();

    Path tmpDir = createTempDir();
    solrHome = exampleHome.toString();

    originalSystemOut = System.out;

    zkDir = tmpDir.resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:{}", zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    // zkClient.close();

    zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build();
    zkClient.makePath("/solr", false, true);

    if (log.isInfoEnabled()) {
      log.info("####SETUP_END {}", getTestName());
    }
  }

  @Test
  public void testPut() throws Exception {
    // test put
    String data = "my data";
    Path localFile = Files.createTempFile("temp", ".data");
    BufferedWriter writer = Files.newBufferedWriter(localFile, StandardCharsets.UTF_8);
    writer.write(data);
    writer.close();

    String[] args =
        new String[] {"cp", "-z", zkServer.getZkAddress(), localFile.toString(), "zk:/data.txt"};

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    assertArrayEquals(
        zkClient.getData("/data.txt", null, null, true), data.getBytes(StandardCharsets.UTF_8));

    // test re-put to existing
    data = "my data deux";

    // Write text to the temporary file
    writer = Files.newBufferedWriter(localFile, StandardCharsets.UTF_8);
    writer.write(data);
    writer.close();

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    assertArrayEquals(
        zkClient.getData("/data.txt", null, null, true), data.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testPutCompressed() throws Exception {
    // test put compressed
    System.setProperty("solr.home", solrHome);
    System.setProperty("minStateByteLenForCompression", "0");

    String data = "my data";

    Path localFile = Files.createTempFile("state", ".json");
    BufferedWriter writer = Files.newBufferedWriter(localFile, StandardCharsets.UTF_8);
    writer.write(data);
    writer.close();

    ZLibCompressor zLibCompressor = new ZLibCompressor();
    byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
    byte[] expected = zLibCompressor.compressBytes(dataBytes);

    String[] args =
        new String[] {"cp", "-z", zkServer.getZkAddress(), localFile.toString(), "zk:/state.json"};

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    assertArrayEquals(dataBytes, zkClient.getCuratorFramework().getData().forPath("/state.json"));
    assertArrayEquals(
        expected, zkClient.getCuratorFramework().getData().undecompressed().forPath("/state.json"));

    // test re-put to existing
    data = "my data deux";
    localFile = Files.createTempFile("state", ".json");
    writer = Files.newBufferedWriter(localFile, StandardCharsets.UTF_8);
    writer.write(data);
    writer.close();

    dataBytes = data.getBytes(StandardCharsets.UTF_8);
    expected = zLibCompressor.compressBytes(dataBytes);

    byte[] fromLoca = new ZLibCompressor().compressBytes(Files.readAllBytes(localFile));
    assertArrayEquals("Should get back what we put in ZK", fromLoca, expected);

    args =
        new String[] {"cp", "-z", zkServer.getZkAddress(), localFile.toString(), "zk:/state.json"};
    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    byte[] fromZkRaw =
        zkClient.getCuratorFramework().getData().undecompressed().forPath("/state.json");
    byte[] fromZk = zkClient.getCuratorFramework().getData().forPath("/state.json");
    byte[] fromLocRaw = Files.readAllBytes(localFile);
    byte[] fromLoc = new ZLibCompressor().compressBytes(fromLocRaw);
    assertArrayEquals(
        "When asking to not decompress, we should get back the compressed data that what we put in ZK",
        fromLoc,
        fromZkRaw);
    assertArrayEquals(
        "When not specifying anything, we should get back what exactly we put in ZK (not compressed)",
        fromLocRaw,
        fromZk);

    assertArrayEquals(dataBytes, zkClient.getCuratorFramework().getData().forPath("/state.json"));
    assertArrayEquals(
        expected, zkClient.getCuratorFramework().getData().undecompressed().forPath("/state.json"));
  }

  @Test
  public void testPutFile() throws Exception {

    String[] args =
        new String[] {
          "cp",
          "-z",
          zkServer.getZkAddress(),
          SOLR_HOME.resolve("solr-stress-new.xml").toString(),
          "zk:/foo.xml"
        };

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    String fromZk =
        new String(zkClient.getData("/foo.xml", null, null, true), StandardCharsets.UTF_8);
    Path localFile = SOLR_HOME.resolve("solr-stress-new.xml");
    String fromLocalFile = Files.readString(localFile);
    assertEquals("Should get back what we put in ZK", fromZk, fromLocalFile);
  }

  @Test
  public void testPutFileWithoutSlash() throws Exception {

    String[] args =
        new String[] {
          "cp",
          "-z",
          zkServer.getZkAddress(),
          SOLR_HOME.resolve("solr-stress-new.xml").toString(),
          "zk:foo.xml"
        };

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    String fromZk =
        new String(zkClient.getData("/foo.xml", null, null, true), StandardCharsets.UTF_8);
    Path localFile = SOLR_HOME.resolve("solr-stress-new.xml");
    String fromLocalFile = Files.readString(localFile);
    assertEquals("Should get back what we put in ZK", fromZk, fromLocalFile);
  }

  @Test
  public void testPutFileCompressed() throws Exception {
    // test put file compressed
    System.setProperty("solr.home", solrHome);
    System.setProperty("minStateByteLenForCompression", "0");

    String[] args =
        new String[] {
          "cp",
          "-z",
          zkServer.getZkAddress(),
          SOLR_HOME.resolve("solr-stress-new.xml").toString(),
          "zk:/state.json"
        };

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    Path locFile = SOLR_HOME.resolve("solr-stress-new.xml");
    byte[] fileBytes = Files.readAllBytes(locFile);

    // Check raw ZK data
    byte[] fromZk =
        zkClient.getCuratorFramework().getData().undecompressed().forPath("/state.json");
    byte[] fromLoc = new ZLibCompressor().compressBytes(fileBytes);
    assertArrayEquals("Should get back a compressed version of what we put in ZK", fromLoc, fromZk);

    // Check curator output (should be decompressed)
    fromZk = zkClient.getCuratorFramework().getData().forPath("/state.json");
    assertArrayEquals(
        "Should get back an uncompressed version what we put in ZK", fileBytes, fromZk);

    // Lets do it again
    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    locFile = SOLR_HOME.resolve("solr-stress-new.xml");
    fileBytes = Files.readAllBytes(locFile);

    fromZk = zkClient.getCuratorFramework().getData().undecompressed().forPath("/state.json");
    fromLoc = new ZLibCompressor().compressBytes(fileBytes);
    assertArrayEquals("Should get back a compressed version of what we put in ZK", fromLoc, fromZk);

    // Check curator output (should be decompressed)
    fromZk = zkClient.getCuratorFramework().getData().forPath("/state.json");
    assertArrayEquals(
        "Should get back an uncompressed version what we put in ZK", fileBytes, fromZk);
  }

  @Test
  public void testPutFileNotExists() throws Exception {

    String[] args =
        new String[] {
          "cp",
          "-z",
          zkServer.getZkAddress(),
          SOLR_HOME.resolve("not-there.xml").toString(),
          "zk:/foo.xml"
        };

    assertEquals(1, CLITestHelper.runTool(args, ZkCpTool.class));
  }

  @Test
  public void testLs() throws Exception {
    zkClient.makePath("/test/path", true);

    // test what happens when path arg "/" isn't the last one.
    String[] args = new String[] {"ls", "/", "-r", "true", "-z", zkServer.getZkAddress()};

    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    assertEquals(0, CLITestHelper.runTool(args, runtime, ZkLsTool.class));

    final String standardOutput2 = runtime.getOutput();

    String separator2 = System.lineSeparator();
    assertEquals(
        "/" + separator2 + "/test" + separator2 + "     path" + separator2 + "/solr" + separator2,
        standardOutput2);
  }

  @Test
  public void testUpConfigLinkConfigClearZk() throws Exception {
    Path tmpDir = createTempDir();

    // test upconfig
    String confsetname = "confsetone";

    String[] args =
        new String[] {
          "upconfig",
          "--conf-name",
          confsetname,
          "--conf-dir",
          ExternalPaths.TECHPRODUCTS_CONFIGSET.toString(),
          "-z",
          zkServer.getZkAddress()
        };

    assertEquals(0, CLITestHelper.runTool(args, ConfigSetUploadTool.class));

    assertTrue(zkClient.exists(ZkConfigSetService.CONFIGS_ZKNODE + "/" + confsetname, true));
    final Path confDir = ExternalPaths.TECHPRODUCTS_CONFIGSET;

    List<String> zkFiles =
        zkClient.getChildren(ZkConfigSetService.CONFIGS_ZKNODE + "/" + confsetname, null, true);

    try (Stream<Path> filesStream = Files.list(confDir)) {
      assertEquals(
          "Verify that all local files are uploaded to ZK", filesStream.count(), zkFiles.size());
    }

    // test linkconfig
    args =
        new String[] {
          "linkconfig",
          "--conf-name",
          confsetname,
          "-c",
          "collection1",
          "-z",
          zkServer.getZkAddress()
        };

    assertEquals(0, CLITestHelper.runTool(args, LinkConfigTool.class));

    ZkNodeProps collectionProps =
        ZkNodeProps.load(
            zkClient.getData(ZkStateReader.COLLECTIONS_ZKNODE + "/collection1", null, null, true));
    assertTrue(collectionProps.containsKey("configName"));
    assertEquals(confsetname, collectionProps.getStr("configName"));

    // test down config
    Path configSetDir =
        tmpDir.resolve(
            "solrtest-confdropspot-" + this.getClass().getName() + "-" + System.nanoTime());
    assertFalse(Files.exists(configSetDir));

    args =
        new String[] {
          "downconfig",
          "--conf-name",
          confsetname,
          "--conf-dir",
          configSetDir.toString(),
          "-z",
          zkServer.getZkAddress()
        };

    assertEquals(0, CLITestHelper.runTool(args, ConfigSetDownloadTool.class));

    Path confSetDir = configSetDir.resolve("conf");
    try (Stream<Path> filesStream = Files.list(confSetDir)) {
      List<Path> files = filesStream.toList();
      zkFiles =
          zkClient.getChildren(ZkConfigSetService.CONFIGS_ZKNODE + "/" + confsetname, null, true);
      assertEquals(
          "Comparing original conf files that were to be uploaded to what is in ZK",
          files.size(),
          zkFiles.size());
      assertEquals("Comparing downloaded files to what is in ZK", files.size(), zkFiles.size());
    }

    Path sourceConfDir = ExternalPaths.TECHPRODUCTS_CONFIGSET;
    // filter out all directories starting with . (e.g. .svn)
    try (Stream<Path> stream = Files.walk(sourceConfDir)) {
      List<Path> files =
          stream
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().startsWith("."))
              .toList();
      files.forEach(
          (sourceFile) -> {
            int indexOfRelativePath =
                sourceFile
                    .toString()
                    .lastIndexOf(
                        "sample_techproducts_configs"
                            + sourceFile.getFileSystem().getSeparator()
                            + "conf");
            String relativePathofFile = sourceFile.toString().substring(indexOfRelativePath + 33);
            Path downloadedFile = confDir.resolve(relativePathofFile);
            if (ConfigSetService.UPLOAD_FILENAME_EXCLUDE_PATTERN
                .matcher(relativePathofFile)
                .matches()) {
              assertFalse(
                  sourceFile + " exists in ZK, downloaded:" + downloadedFile,
                  Files.exists(downloadedFile));
            } else {
              assertTrue(
                  downloadedFile + " does not exist source:" + sourceFile,
                  Files.exists(downloadedFile));
              try {
                assertEquals(
                    relativePathofFile + " content changed",
                    -1,
                    Files.mismatch(sourceFile, downloadedFile));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          });
    }

    // test reset zk
    args = new String[] {"rm", "-r", "-z", zkServer.getZkAddress(), "zk:/configs/confsetone"};

    assertEquals(0, CLITestHelper.runTool(args, ZkRmTool.class));

    assertEquals(0, zkClient.getChildren("/configs", null, true).size());
  }

  @Test
  public void testGet() throws Exception {
    String getNode = "/getNode";
    byte[] data = "getNode-data".getBytes(StandardCharsets.UTF_8);
    zkClient.create(getNode, data, CreateMode.PERSISTENT, true);

    Path localFile = Files.createTempFile("temp", ".data");

    String[] args =
        new String[] {"cp", "-z", zkServer.getZkAddress(), "zk:" + getNode, localFile.toString()};

    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    assertEquals(0, CLITestHelper.runTool(args, runtime, ZkCpTool.class));

    final String standardOutput2 = runtime.getOutput();
    assertTrue(standardOutput2.startsWith("Copying from 'zk:/getNode'"));
    byte[] fileBytes = Files.readAllBytes(localFile);
    assertArrayEquals(data, fileBytes);
  }

  @Test
  public void testGetCompressed() throws Exception {
    System.setProperty("solr.home", solrHome);
    System.setProperty("minStateByteLenForCompression", "0");

    String getNode = "/getNode";
    byte[] data = "getNode-data".getBytes(StandardCharsets.UTF_8);
    ZLibCompressor zLibCompressor = new ZLibCompressor();
    byte[] compressedData =
        random().nextBoolean()
            ? zLibCompressor.compressBytes(data)
            : zLibCompressor.compressBytes(data, data.length / 10);
    zkClient.create(getNode, compressedData, CreateMode.PERSISTENT, true);

    Path localFile = Files.createTempFile("temp", ".data");

    String[] args =
        new String[] {"cp", "-z", zkServer.getZkAddress(), "zk:" + getNode, localFile.toString()};

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    assertArrayEquals(data, Files.readAllBytes(localFile));
  }

  @Test
  public void testGetFile() throws Exception {
    Path tmpDir = createTempDir();

    String getNode = "/getFileNode";
    byte[] data = "getFileNode-data".getBytes(StandardCharsets.UTF_8);
    zkClient.create(getNode, data, CreateMode.PERSISTENT, true);

    Path file =
        tmpDir.resolve("solrtest-getfile-" + this.getClass().getName() + "-" + System.nanoTime());

    String[] args =
        new String[] {"cp", "-z", zkServer.getZkAddress(), "zk:" + getNode, file.toString()};

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));
    assertArrayEquals(data, Files.readAllBytes(file));
  }

  @Test
  public void testGetFileCompressed() throws Exception {
    Path tmpDir = createTempDir();

    String getNode = "/getFileNode";
    byte[] data = "getFileNode-data".getBytes(StandardCharsets.UTF_8);
    ZLibCompressor zLibCompressor = new ZLibCompressor();
    byte[] compressedData =
        random().nextBoolean()
            ? zLibCompressor.compressBytes(data)
            : zLibCompressor.compressBytes(data, data.length / 10);
    zkClient.create(getNode, compressedData, CreateMode.PERSISTENT, true);

    Path file =
        tmpDir.resolve("solrtest-getfile-" + this.getClass().getName() + "-" + System.nanoTime());

    String[] args =
        new String[] {"cp", "-z", zkServer.getZkAddress(), "zk:" + getNode, file.toString()};

    assertEquals(0, CLITestHelper.runTool(args, ZkCpTool.class));

    assertArrayEquals(data, Files.readAllBytes(file));
  }

  @Test
  public void testGetFileNotExists() throws Exception {
    String getNode = "/getFileNotExistsNode";

    Path file = createTempFile("newfile", null);

    String[] args =
        new String[] {"cp", "-z", zkServer.getZkAddress(), "zk:" + getNode, file.toString()};

    assertEquals(1, CLITestHelper.runTool(args, ZkCpTool.class));
  }

  @Test
  public void testInvalidZKAddress() throws Exception {

    String[] args = new String[] {"ls", "/", "-r", "-z", "----------:33332"};

    assertEquals(1, CLITestHelper.runTool(args, ZkLsTool.class));
  }

  @Test
  public void testSetClusterProperty() throws Exception {
    ClusterProperties properties = new ClusterProperties(zkClient);
    // add property urlScheme=http
    String[] args =
        new String[] {
          "cluster", "--property", "urlScheme", "--value", "http", "-z", zkServer.getZkAddress()
        };
    assertEquals(0, CLITestHelper.runTool(args, ClusterTool.class));

    assertEquals("http", properties.getClusterProperty("urlScheme", "none"));

    args = new String[] {"cluster", "--property", "urlScheme", "-z", zkServer.getZkAddress()};
    assertEquals(0, CLITestHelper.runTool(args, ClusterTool.class));
    assertNull(properties.getClusterProperty("urlScheme", (String) null));
  }

  @Test
  public void testUpdateAcls() throws Exception {
    try {
      System.setProperty(
          SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
          DigestZkACLProvider.class.getName());
      System.setProperty(
          VMParamsZkCredentialsInjector.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME, "user");
      System.setProperty(
          VMParamsZkCredentialsInjector.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME, "pass");
      System.setProperty(
          SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
          VMParamsAllAndReadonlyDigestZkACLProvider.class.getName());
      System.setProperty(
          VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
          "user");
      System.setProperty(
          VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME,
          "pass");

      String[] args = new String[] {"updateacls", "/", "-z", zkServer.getZkAddress()};
      assertEquals(0, CLITestHelper.runTool(args, UpdateACLTool.class));
    } finally {
      // Need to clear these before we open the next SolrZkClient
      System.clearProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
      System.clearProperty(
          VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME);
      System.clearProperty(
          VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);
    }

    boolean excepted = false;
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(
                AbstractDistribZkTestBase.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      zkClient.getData("/", null, null, true);
    } catch (KeeperException.NoAuthException e) {
      excepted = true;
    }
    assertTrue("Did not fail to read.", excepted);
  }

  @Override
  public void tearDown() throws Exception {
    if (zkClient != null) {
      zkClient.close();
    }
    if (zkServer != null) {
      zkServer.shutdown();
    }
    System.clearProperty("solr.home");
    System.clearProperty("minStateByteLenForCompression");
    System.setOut(originalSystemOut);
    super.tearDown();
  }
}

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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrCLIZkToolsTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    zkAddr = cluster.getZkServer().getZkAddress();
    zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkAddr)
            .withTimeout(30000, TimeUnit.MILLISECONDS)
            .build();
    System.setProperty("solr.solr.home", TEST_HOME());
  }

  @AfterClass
  public static void closeConn() {
    if (null != zkClient) {
      zkClient.close();
      zkClient = null;
    }
    zkAddr = null;
  }

  private static String zkAddr;
  private static SolrZkClient zkClient;

  @Test
  public void testUpconfig() throws Exception {
    // Use a full, explicit path for configset.

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");
    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "upconfig1", zkAddr);
    // Now do we have that config up on ZK?
    verifyZkLocalPathsMatch(srcPathCheck, "/configs/upconfig1");

    // Now just use a name in the configsets directory, do we find it?
    configSet = TEST_PATH().resolve("configsets");

    File confDir = new File(configSet.toFile(), "cloud-subdirs");
    String[] args =
        new String[] {
          "--conf-name", "upconfig2", "--conf-dir", confDir.getAbsolutePath(), "-z", zkAddr
        };

    ConfigSetUploadTool tool = new ConfigSetUploadTool();

    int res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    assertEquals("tool should have returned 0 for success ", 0, res);
    // Now do we have that config up on ZK?
    verifyZkLocalPathsMatch(srcPathCheck, "/configs/upconfig2");

    // do we barf on a bogus path?
    args = new String[] {"--conf-name", "upconfig3", "--conf-dir", "nothinghere", "-z", zkAddr};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    assertTrue("tool should have returned non-zero for failure ", 0 != res);

    String content =
        new String(
            zkClient.getData("/configs/upconfig2/schema.xml", null, null, true),
            StandardCharsets.UTF_8);
    assertTrue(
        "There should be content in the node! ", content.contains("Apache Software Foundation"));
  }

  @Test
  public void testDownconfig() throws Exception {
    Path tmp =
        Paths.get(createTempDir("downConfigNewPlace").toAbsolutePath().toString(), "myconfset");

    // First we need a configset on ZK to bring down.

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");
    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "downconfig1", zkAddr);
    // Now do we have that config up on ZK?
    verifyZkLocalPathsMatch(srcPathCheck, "/configs/downconfig1");

    String[] args =
        new String[] {
          "--conf-name", "downconfig1", "--conf-dir", tmp.toAbsolutePath().toString(), "-z", zkAddr,
        };

    ConfigSetDownloadTool downTool = new ConfigSetDownloadTool();
    int res = downTool.runTool(SolrCLI.processCommandLineArgs(downTool, args));
    assertEquals("Download should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(
        Paths.get(tmp.toAbsolutePath().toString(), "conf"), "/configs/downconfig1");

    // Ensure that empty files don't become directories (SOLR-11198)

    Path emptyFile = Paths.get(tmp.toAbsolutePath().toString(), "conf", "stopwords", "emptyfile");
    Files.createFile(emptyFile);

    // Now copy it up and back and insure it's still a file in the new place
    AbstractDistribZkTestBase.copyConfigUp(tmp.getParent(), "myconfset", "downconfig2", zkAddr);
    Path tmp2 = createTempDir("downConfigNewPlace2");
    downTool = new ConfigSetDownloadTool();
    args =
        new String[] {
          "--conf-name",
          "downconfig2",
          "--conf-dir",
          tmp2.toAbsolutePath().toString(),
          "-z",
          zkAddr,
        };

    res = downTool.runTool(SolrCLI.processCommandLineArgs(downTool, args));
    assertEquals("Download should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(
        Paths.get(tmp.toAbsolutePath().toString(), "conf"), "/configs/downconfig2");
    // And insure the empty file is a text file
    Path destEmpty = Paths.get(tmp2.toAbsolutePath().toString(), "conf", "stopwords", "emptyfile");
    assertTrue("Empty files should NOT be copied down as directories", destEmpty.toFile().isFile());
  }

  @Test
  public void testCp() throws Exception {
    // First get something up on ZK

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "cp1", zkAddr);

    // Now copy it somewhere else on ZK.
    String[] args = new String[] {"--recurse", "--zk-host", zkAddr, "zk:/configs/cp1", "zk:/cp2"};

    ZkCpTool cpTool = new ZkCpTool();

    int res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy from zk -> zk should have succeeded.", 0, res);
    verifyZnodesMatch("/configs/cp1", "/cp2");

    // try with zk->local
    Path tmp = createTempDir("tmpNewPlace2");
    args =
        new String[] {
          "--recurse", "--zk-host", zkAddr, "zk:/configs/cp1", "file:" + tmp.toAbsolutePath()
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(tmp, "/configs/cp1");

    // try with zk->local  no file: prefix
    tmp = createTempDir("tmpNewPlace3");
    args =
        new String[] {
          "--recurse", "--zk-host", zkAddr, "zk:/configs/cp1", tmp.toAbsolutePath().toString()
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(tmp, "/configs/cp1");

    // try with local->zk
    args =
        new String[] {
          "--recurse", "--zk-host", zkAddr, srcPathCheck.toAbsolutePath().toString(), "zk:/cp3"
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(srcPathCheck, "/cp3");

    // try with local->zk, file: specified
    args =
        new String[] {
          "--recurse", "--zk-host", zkAddr, "file:" + srcPathCheck.toAbsolutePath(), "zk:/cp4"
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(srcPathCheck, "/cp4");

    // try with recurse not specified, and therefore not happening
    args =
        new String[] {"--zk-host", zkAddr, "file:" + srcPathCheck.toAbsolutePath(), "zk:/cp5Fail"};

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertTrue("Copy should NOT have succeeded, recurse not specified.", 0 != res);

    // NOTE: really can't test copying to '.' because the test framework doesn't allow altering the
    // source tree and at least IntelliJ's CWD is in the source tree.

    // copy to local ending in separator
    // src and cp3 and cp4 are valid
    String localSlash = tmp.normalize() + File.separator + "cpToLocal" + File.separator;
    args = new String[] {"--zk-host", zkAddr, "zk:/cp3/schema.xml", localSlash};

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should nave created intermediate directory locally.", 0, res);
    assertTrue(
        "File should have been copied to a directory successfully",
        Files.exists(Paths.get(localSlash, "schema.xml")));

    // copy to ZK ending in '/'.
    // src and cp3 are valid
    args =
        new String[] {
          "--zk-host",
          zkAddr,
          "file:" + srcPathCheck.normalize().toAbsolutePath() + File.separator + "solrconfig.xml",
          "zk:/powerup/"
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy up to intermediate file should have succeeded.", 0, res);
    assertTrue(
        "Should have created an intermediate node on ZK",
        zkClient.exists("/powerup/solrconfig.xml", true));

    // copy individual file up
    // src and cp3 are valid
    args =
        new String[] {
          "--zk-host",
          zkAddr,
          "file:" + srcPathCheck.normalize().toAbsolutePath() + File.separator + "solrconfig.xml",
          "zk:/copyUpFile.xml"
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy up to named file should have succeeded.", 0, res);
    assertTrue(
        "Should NOT have created an intermediate node on ZK",
        zkClient.exists("/copyUpFile.xml", true));

    // copy individual file down
    // src and cp3 are valid

    String localNamed =
        tmp.normalize() + File.separator + "localnamed" + File.separator + "renamed.txt";
    args = new String[] {"--zk-host", zkAddr, "zk:/cp4/solrconfig.xml", "file:" + localNamed};

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy to local named file should have succeeded.", 0, res);
    Path locPath = Paths.get(localNamed);
    assertTrue("Should have found file: " + localNamed, Files.exists(locPath));
    assertTrue("Should be an individual file", Files.isRegularFile(locPath));
    assertTrue("File should have some data", Files.size(locPath) > 100);
    boolean foundApache = false;
    for (String line : Files.readAllLines(locPath, StandardCharsets.UTF_8)) {
      if (line.contains("Apache Software Foundation")) {
        foundApache = true;
        break;
      }
    }
    assertTrue("Should have found Apache Software Foundation in the file! ", foundApache);

    // Test copy from somwehere in ZK to the root of ZK.
    args = new String[] {"--zk-host", zkAddr, "zk:/cp4/solrconfig.xml", "zk:/"};

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy from somewhere in ZK to ZK root should have succeeded.", 0, res);
    assertTrue(
        "Should have found znode /solrconfig.xml: ", zkClient.exists("/solrconfig.xml", true));

    // Check that the form path/ works for copying files up. Should append the last bit of the
    // source path to the dst
    args =
        new String[] {
          "--recurse", "--zk-host", zkAddr, "file:" + srcPathCheck.toAbsolutePath(), "zk:/cp7/"
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);
    verifyZkLocalPathsMatch(srcPathCheck, "/cp7/" + srcPathCheck.getFileName().toString());

    // Check for an intermediate ZNODE having content. You know cp7/stopwords is a parent node.
    tmp = createTempDir("dirdata");
    Path file = Paths.get(tmp.toAbsolutePath().toString(), "zknode.data");
    List<String> lines = new ArrayList<>();
    lines.add("{Some Arbitrary Data}");
    Files.write(file, lines, StandardCharsets.UTF_8);
    // First, just copy the data up the cp7 since it's a directory.
    args =
        new String[] {
          "--zk-host", zkAddr, "file:" + file.toAbsolutePath(), "zk:/cp7/conf/stopwords/"
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);

    String content =
        new String(
            zkClient.getData("/cp7/conf/stopwords", null, null, true), StandardCharsets.UTF_8);
    assertTrue("There should be content in the node! ", content.contains("{Some Arbitrary Data}"));

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);

    tmp = createTempDir("cp8");
    args =
        new String[] {"--recurse", "--zk-host", zkAddr, "zk:/cp7", "file:" + tmp.toAbsolutePath()};
    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);

    // Next, copy cp7 down and verify that zknode.data exists for cp7
    Path zData = Paths.get(tmp.toAbsolutePath().toString(), "conf/stopwords/zknode.data");
    assertTrue("znode.data should have been copied down", zData.toFile().exists());

    // Finally, copy up to cp8 and verify that the data is up there.
    args =
        new String[] {"--recurse", "--zk-host", zkAddr, "file:" + tmp.toAbsolutePath(), "zk:/cp9"};

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);

    content =
        new String(
            zkClient.getData("/cp9/conf/stopwords", null, null, true), StandardCharsets.UTF_8);
    assertTrue("There should be content in the node! ", content.contains("{Some Arbitrary Data}"));

    // Copy an individual empty file up and back down and insure it's still a file
    Path emptyFile = Paths.get(tmp.toAbsolutePath().toString(), "conf", "stopwords", "emptyfile");
    Files.createFile(emptyFile);

    args =
        new String[] {
          "--zk-host",
          zkAddr,
          "file:" + emptyFile.toAbsolutePath(),
          "zk:/cp7/conf/stopwords/emptyfile"
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);

    Path tmp2 = createTempDir("cp9");
    Path emptyDest = Paths.get(tmp2.toAbsolutePath().toString(), "emptyfile");
    args =
        new String[] {
          "--zk-host",
          zkAddr,
          "zk:/cp7/conf/stopwords/emptyfile",
          "file:" + emptyDest.toAbsolutePath()
        };
    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);

    assertTrue("Empty files should NOT be copied down as directories", emptyDest.toFile().isFile());

    // Now with recursive copy

    args =
        new String[] {
          "--recurse",
          "--zk-host",
          zkAddr,
          "file:" + emptyFile.getParent().getParent().toString(),
          "zk:/cp10"
        };

    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);

    // Now copy it all back and make sure empty file is still a file when recursively copying.
    tmp2 = createTempDir("cp10");
    args =
        new String[] {
          "--recurse", "--zk-host", zkAddr, "zk:/cp10", "file:" + tmp2.toAbsolutePath()
        };
    res = cpTool.runTool(SolrCLI.processCommandLineArgs(cpTool, args));
    assertEquals("Copy should have succeeded.", 0, res);

    Path locEmpty = Paths.get(tmp2.toAbsolutePath().toString(), "stopwords", "emptyfile");
    assertTrue("Empty files should NOT be copied down as directories", locEmpty.toFile().isFile());
  }

  @Test
  public void testMv() throws Exception {

    // First get something up on ZK

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "mv1", zkAddr);

    // Now move it somewhere else.
    String[] args = new String[] {"--zk-host", zkAddr, "zk:/configs/mv1", "zk:/mv2"};

    ZkMvTool mvTool = new ZkMvTool();

    int res = mvTool.runTool(SolrCLI.processCommandLineArgs(mvTool, args));
    assertEquals("Move should have succeeded.", 0, res);

    // Now does the moved directory match the original on disk?
    verifyZkLocalPathsMatch(srcPathCheck, "/mv2");
    // And are we sure the old path is gone?
    assertFalse("/configs/mv1 Znode should not be there: ", zkClient.exists("/configs/mv1", true));

    // Files are in mv2
    // Now fail if we specify "file:". Everything should still be in /mv2
    args = new String[] {"--zk-host", zkAddr, "file:" + File.separator + "mv2", "/mv3"};

    // Still in mv2
    res = mvTool.runTool(SolrCLI.processCommandLineArgs(mvTool, args));
    assertTrue("Move should NOT have succeeded with file: specified.", 0 != res);

    // Let's move it to yet another place with no zk: prefix.
    args = new String[] {"--zk-host", zkAddr, "/mv2", "/mv4"};

    res = mvTool.runTool(SolrCLI.processCommandLineArgs(mvTool, args));
    assertEquals("Move should have succeeded.", 0, res);

    assertFalse("Znode /mv3 really should be gone", zkClient.exists("/mv3", true));

    // Now does the moved directory match the original on disk?
    verifyZkLocalPathsMatch(srcPathCheck, "/mv4");

    args = new String[] {"-z", zkAddr, "/mv4/solrconfig.xml", "/testmvsingle/solrconfig.xml"};

    res = mvTool.runTool(SolrCLI.processCommandLineArgs(mvTool, args));
    assertEquals("Move should have succeeded.", 0, res);
    assertTrue(
        "Should be able to move a single file",
        zkClient.exists("/testmvsingle/solrconfig.xml", true));

    zkClient.makePath("/parentNode", true);

    // what happens if the destination ends with a slash?
    args = new String[] {"--zk-host", zkAddr, "/mv4/schema.xml", "/parentnode/"};

    res = mvTool.runTool(SolrCLI.processCommandLineArgs(mvTool, args));
    assertEquals("Move should have succeeded.", 0, res);
    assertTrue(
        "Should be able to move a single file to a parent znode",
        zkClient.exists("/parentnode/schema.xml", true));
    String content =
        new String(
            zkClient.getData("/parentnode/schema.xml", null, null, true), StandardCharsets.UTF_8);
    assertTrue(
        "There should be content in the node! ", content.contains("Apache Software Foundation"));
  }

  @Test
  public void testLs() throws Exception {

    Path configSet = TEST_PATH().resolve("configsets");

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "lister", zkAddr);

    // Should only find a single level.
    String[] args = new String[] {"--zk-host", zkAddr, "/configs"};

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos, false, StandardCharsets.UTF_8.name());
    ZkLsTool tool = new ZkLsTool(ps);

    int res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    String content = baos.toString(StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertFalse("Return should NOT contain a child node", content.contains("solrconfig.xml"));

    // simple ls with no recursion
    args = new String[] {"--zk-host", zkAddr, "/configs"};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    content = baos.toString(StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertFalse("Return should NOT contain a child node", content.contains("solrconfig.xml"));

    // ls with recursion
    args = new String[] {"--recurse", "--zk-host", zkAddr, "/configs"};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    content = baos.toString(StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertTrue("Return should contain a child node", content.contains("solrconfig.xml"));

    // Saw a case where going from root didn't work, so test it.
    args = new String[] {"--recurse", "--zk-host", zkAddr, "/"};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    content = baos.toString(StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertTrue("Return should contain a child node", content.contains("solrconfig.xml"));

    args = new String[] {"--zk-host", zkAddr, "/"};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    content = baos.toString(StandardCharsets.UTF_8);
    assertEquals("List should have succeeded", res, 0);
    assertFalse("Return should not contain /zookeeper", content.contains("/zookeeper"));

    // Saw a case where ending in slash didn't work, so test it.
    args = new String[] {"--recurse", "--zk-host", zkAddr, "/configs/"};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    content = baos.toString(StandardCharsets.UTF_8);

    assertEquals("List should have succeeded", res, 0);
    assertTrue("Return should contain the conf directory", content.contains("lister"));
    assertTrue("Return should contain a child node", content.contains("solrconfig.xml"));
  }

  @Test
  public void testRm() throws Exception {

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "rm1", zkAddr);
    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "rm2", zkAddr);

    // Should fail if recurse not set.
    String[] args = new String[] {"--zk-host", zkAddr, "/configs/rm1"};

    ZkRmTool tool = new ZkRmTool();

    int res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));

    assertTrue("Should have failed to remove node with children unless --recurse is set", res != 0);

    // Are we sure all the znodes are still there?
    verifyZkLocalPathsMatch(srcPathCheck, "/configs/rm1");

    // run without recurse specified
    args = new String[] {"--zk-host", zkAddr, "zk:/configs/rm1"};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));

    assertTrue(
        "Should have failed to remove node with children if --recurse is set to false", res != 0);

    args = new String[] {"--recurse", "--zk-host", zkAddr, "/configs/rm1"};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    assertEquals("Should have removed node /configs/rm1", res, 0);
    assertFalse(
        "Znode /configs/toremove really should be gone", zkClient.exists("/configs/rm1", true));

    // Check that zk prefix also works.
    args = new String[] {"--recurse", "--zk-host", zkAddr, "zk:/configs/rm2"};

    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    assertEquals("Should have removed node /configs/rm2", res, 0);
    assertFalse(
        "Znode /configs/toremove2 really should be gone", zkClient.exists("/configs/rm2", true));

    // This should silently just refuse to do anything to the / or /zookeeper
    args = new String[] {"--recurse", "--zk-host", zkAddr, "zk:/"};

    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "rm3", zkAddr);
    res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    assertNotEquals("Should fail when trying to remove /.", 0, res);
  }

  // Check that all children of fileRoot are children of zkRoot and vice-versa
  private void verifyZkLocalPathsMatch(Path fileRoot, String zkRoot)
      throws IOException, KeeperException, InterruptedException {
    verifyAllFilesAreZNodes(fileRoot, zkRoot);
    verifyAllZNodesAreFiles(fileRoot, zkRoot);
  }

  private static boolean isEphemeral(String zkPath) throws KeeperException, InterruptedException {
    Stat znodeStat = zkClient.exists(zkPath, null, true);
    return znodeStat.getEphemeralOwner() != 0;
  }

  void verifyAllZNodesAreFiles(Path fileRoot, String zkRoot)
      throws KeeperException, InterruptedException {

    for (String child : zkClient.getChildren(zkRoot, null, true)) {
      // Skip ephemeral nodes
      if (!zkRoot.endsWith("/")) {
        zkRoot += "/";
      }
      if (isEphemeral(zkRoot + child)) continue;

      Path thisPath = Paths.get(fileRoot.toAbsolutePath().toString(), child);
      assertTrue(
          "Znode " + child + " should have been found on disk at " + fileRoot.toAbsolutePath(),
          Files.exists(thisPath));
      verifyAllZNodesAreFiles(thisPath, zkRoot + child);
    }
  }

  void verifyAllFilesAreZNodes(Path fileRoot, String zkRoot) throws IOException {
    Files.walkFileTree(
        fileRoot,
        new SimpleFileVisitor<Path>() {
          void checkPathOnZk(Path path) {
            String znode = ZkMaintenanceUtils.createZkNodeName(zkRoot, fileRoot, path);
            try { // It's easier to catch this exception and fail than catch it everywhere else.
              assertTrue(
                  "Should have found " + znode + " on Zookeeper", zkClient.exists(znode, true));
            } catch (Exception e) {
              fail(
                  "Caught unexpected exception "
                      + e.getMessage()
                      + " Znode we were checking "
                      + znode);
            }
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            assertTrue("Path should start at proper place!", file.startsWith(fileRoot));
            checkPathOnZk(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {

            checkPathOnZk(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  // Ensure that all znodes in first are in second and vice-versa
  private void verifyZnodesMatch(String first, String second)
      throws KeeperException, InterruptedException {
    verifyFirstZNodesInSecond(first, second);
    verifyFirstZNodesInSecond(second, first);
  }

  // Note, no fuss here with Windows path names.
  private void verifyFirstZNodesInSecond(String first, String second)
      throws KeeperException, InterruptedException {
    for (String node : zkClient.getChildren(first, null, true)) {
      String fNode = first + "/" + node;
      String sNode = second + "/" + node;
      assertTrue("Node " + sNode + " not found. Exists on " + fNode, zkClient.exists(sNode, true));
      verifyFirstZNodesInSecond(fNode, sNode);
    }
  }
}

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
package org.apache.solr.common.cloud;

import com.google.common.base.Throwables;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.ZkConfigSetService;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

@LogLevel("org.apache.solr.common.cloud=DEBUG")
public class TestZkConfigSetService extends SolrTestCaseJ4 {

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

  @Test
  public void testConstants() throws Exception {
    assertEquals("/configs", ZkConfigSetService.CONFIGS_ZKNODE);
    assertEquals("^\\..*$", ConfigSetService.UPLOAD_FILENAME_EXCLUDE_REGEX);
  }

  @Test
  public void testUploadConfig() throws IOException {

    zkServer.ensurePathExists("/solr");

    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress("/solr"), 10000)) {
      ConfigSetService configSetService = new ZkConfigSetService(zkClient);

      assertEquals(0, configSetService.listConfigs().size());

      byte[] testdata = "test data".getBytes(StandardCharsets.UTF_8);

      Path tempConfig = createTempDir("config");
      Files.createFile(tempConfig.resolve("file1"));
      Files.write(tempConfig.resolve("file1"), testdata);
      Files.createFile(tempConfig.resolve("file2"));
      Files.createDirectory(tempConfig.resolve("subdir"));
      Files.createFile(tempConfig.resolve("subdir").resolve("file3"));
      Files.createFile(tempConfig.resolve(".ignored"));
      Files.createDirectory(tempConfig.resolve(".ignoreddir"));
      Files.createFile(tempConfig.resolve(".ignoreddir").resolve("ignored"));

      configSetService.uploadConfig("testconfig", tempConfig);

      // uploading a directory creates a new config
      List<String> configs = configSetService.listConfigs();
      assertEquals(1, configs.size());
      assertEquals("testconfig", configs.get(0));

      // check downloading
      Path downloadPath = createTempDir("download");
      configSetService.downloadConfig( "testconfig", downloadPath);
      assertTrue(Files.exists(downloadPath.resolve("file1")));
      assertTrue(Files.exists(downloadPath.resolve("file2")));
      assertTrue(Files.isDirectory(downloadPath.resolve("subdir")));
      assertTrue(Files.exists(downloadPath.resolve("subdir/file3")));
      // dotfiles should be ignored
      assertFalse(Files.exists(downloadPath.resolve(".ignored")));
      assertFalse(Files.exists(downloadPath.resolve(".ignoreddir/ignored")));
      byte[] checkdata = Files.readAllBytes(downloadPath.resolve("file1"));
      assertArrayEquals(testdata, checkdata);

      // uploading to the same config overwrites
      byte[] overwritten = "new test data".getBytes(StandardCharsets.UTF_8);
      Files.write(tempConfig.resolve("file1"), overwritten);
      configSetService.uploadConfig("testconfig", tempConfig);

      assertEquals(1, configSetService.listConfigs().size());
      Path download2 = createTempDir("download2");
      configSetService.downloadConfig( "testconfig", download2);
      byte[] checkdata2 = Files.readAllBytes(download2.resolve("file1"));
      assertArrayEquals(overwritten, checkdata2);

      // uploading same files to a new name creates a new config
      configSetService.uploadConfig("config2", tempConfig);
      assertEquals(2, configSetService.listConfigs().size());

      // Test copying a config works in both flavors
      configSetService.copyConfig("config2", "config2copy");
      configSetService.copyConfig( "config2", "config2copy2");
      configs = configSetService.listConfigs();
      assertTrue("config2copy should exist", configs.contains("config2copy"));
      assertTrue("config2copy2 should exist", configs.contains("config2copy2"));
    }
  }

  @Test
  public void testUploadWithACL() throws IOException {

    zkServer.ensurePathExists("/acl");

    final String readOnlyUsername = "readonly";
    final String readOnlyPassword = "readonly";
    final String writeableUsername = "writeable";
    final String writeablePassword = "writeable";

    ZkACLProvider aclProvider = new DefaultZkACLProvider(){
      @Override
      protected List<ACL> createGlobalACLsToAdd() {
        try {
          List<ACL> result = new ArrayList<>();
          result.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest(writeableUsername + ":" + writeablePassword))));
          result.add(new ACL(ZooDefs.Perms.READ, new Id("digest", DigestAuthenticationProvider.generateDigest(readOnlyUsername + ":" + readOnlyPassword))));
          return result;
        }
        catch (NoSuchAlgorithmException e) {
          throw new RuntimeException(e);
        }
      }
    };

    ZkCredentialsProvider readonly = new DefaultZkCredentialsProvider(){
      @Override
      protected Collection<ZkCredentials> createCredentials() {
        List<ZkCredentials> credentials = new ArrayList<>();
        credentials.add(new ZkCredentials("digest", (readOnlyUsername + ":" + readOnlyPassword).getBytes(StandardCharsets.UTF_8)));
        return credentials;
      }
    };

    ZkCredentialsProvider writeable = new DefaultZkCredentialsProvider(){
      @Override
      protected Collection<ZkCredentials> createCredentials() {
        List<ZkCredentials> credentials = new ArrayList<>();
        credentials.add(new ZkCredentials("digest", (writeableUsername + ":" + writeablePassword).getBytes(StandardCharsets.UTF_8)));
        return credentials;
      }
    };

    Path configPath = createTempDir("acl-config");
    Files.createFile(configPath.resolve("file1"));

    // Start with all-access client
    try (SolrZkClient client = buildZkClient(zkServer.getZkAddress("/acl"), aclProvider, writeable)) {
      ConfigSetService configSetService = new ZkConfigSetService(client);
      configSetService.uploadConfig("acltest", configPath);
      assertEquals(1, configSetService.listConfigs().size());
    }

    // Readonly access client can get the list of configs, but can't upload
    try (SolrZkClient client = buildZkClient(zkServer.getZkAddress("/acl"), aclProvider, readonly)) {
      ConfigSetService configSetService = new ZkConfigSetService(client);
      assertEquals(1, configSetService.listConfigs().size());
      configSetService.uploadConfig("acltest2", configPath);
      fail ("Should have thrown an ACL exception");
    }
    catch (IOException e) {
      assertEquals(KeeperException.NoAuthException.class, Throwables.getRootCause(e).getClass());
    }

    // Client with no auth whatsoever can't even get the list of configs
    try (SolrZkClient client = new SolrZkClient(zkServer.getZkAddress("/acl"), 10000)) {
      ConfigSetService configSetService = new ZkConfigSetService(client);
      configSetService.listConfigs();
      fail("Should have thrown an ACL exception");
    }
    catch (IOException e) {
      assertEquals(KeeperException.NoAuthException.class, Throwables.getRootCause(e).getClass());
    }

  }

  @Test
  public void testBootstrapConf() throws IOException, KeeperException, InterruptedException {

    String solrHome = SolrJettyTestBase.legacyExampleCollection1SolrHome();

    CoreContainer cc = new CoreContainer(Paths.get(solrHome), new Properties());
    System.setProperty("zkHost", zkServer.getZkAddress());

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr", false, true);
    cc.setCoreConfigService(new ZkConfigSetService(zkClient));
    assertFalse(cc.getConfigSetService().checkConfigExists("collection1"));
    ConfigSetService.bootstrapConf(cc);
    assertTrue(cc.getConfigSetService().checkConfigExists("collection1"));

    zkClient.close();
  }


  static SolrZkClient buildZkClient(String zkAddress, final ZkACLProvider aclProvider,
                                    final ZkCredentialsProvider credentialsProvider) {
    return new SolrZkClient(zkAddress, 10000){
      @Override
      protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
        return credentialsProvider;
      }

      @Override
      protected ZkACLProvider createZkACLProvider() {
        return aclProvider;
      }
    };
  }

}

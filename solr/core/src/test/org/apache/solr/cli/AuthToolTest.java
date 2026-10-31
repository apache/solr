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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.util.Constants;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.security.Sha256AuthenticationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthToolTest extends SolrCloudTestCase {
  private Path dir;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    dir = createTempDir("AuthToolTest").toAbsolutePath();
    // Reset ZK security state before each test to avoid interference between tests
    if (cluster.getZkClient().exists("/security.json")) {
      cluster.getZkClient().setData("/security.json", "{}".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (null != dir) {
      PathUtils.deleteDirectory(dir);
      dir = null;
    }
  }

  protected int runTool(String[] args, Class<? extends ToolBase> clazz) throws Exception {
    return CLITestHelper.runTool(args, clazz);
  }

  @Test
  public void testEnableAuth() throws Exception {
    Path solrIncludeFile = Files.createFile(dir.resolve("solrIncludeFile.txt"));
    String sslSetting = Constants.WINDOWS ? "set SOLR_SSL_ENABLED=true" : "SOLR_SSL_ENABLED=true";
    Files.writeString(solrIncludeFile, sslSetting + "\n");
    String[] args = {
      "auth",
      "enable",
      "-z",
      cluster.getZkClient().getZkServerAddress(),
      "--auth-conf-dir",
      dir.toAbsolutePath().toString(),
      "--solr-include-file",
      solrIncludeFile.toAbsolutePath().toString(),
      "--credentials",
      "solr:solrRocks",
      "--block-unknown",
      "true"
    };
    assertEquals(0, runTool(args, AuthTool.class));
    String enabled = Files.readString(solrIncludeFile);
    assertTrue(enabled.contains(sslSetting));
    assertTrue(
        enabled.contains(
            Constants.WINDOWS ? "set SOLR_AUTH_TYPE=basic" : "SOLR_AUTH_TYPE='basic'"));
    assertTrue(enabled.contains("-Dsolr.httpclient.config=" + dir.resolve("basicAuth.conf")));
    assertEquals(
        "httpBasicAuthUser=solr\nhttpBasicAuthPassword=solrRocks",
        Files.readString(dir.resolve("basicAuth.conf")));
    String[] disable = {
      "auth",
      "disable",
      "-z",
      cluster.getZkClient().getZkServerAddress(),
      "--auth-conf-dir",
      dir.toString(),
      "--solr-include-file",
      solrIncludeFile.toString()
    };
    assertEquals(0, runTool(disable, AuthTool.class));
    String disabled = Files.readString(solrIncludeFile);
    assertTrue(disabled.contains(sslSetting));
    assertTrue(
        disabled.contains(
            Constants.WINDOWS ? "REM set SOLR_AUTH_TYPE=basic" : "# SOLR_AUTH_TYPE='basic'"));
    assertEquals(
        "{}",
        new String(
            cluster.getZkClient().getData("/security.json", null, null), StandardCharsets.UTF_8));
  }

  @Test
  public void testEnableAuthRejectsUsernameEqualPassword() throws Exception {
    Path solrIncludeFile = Files.createFile(dir.resolve("solrIncludeFile2.txt"));
    String[] args = {
      "auth",
      "enable",
      "-z",
      cluster.getZkClient().getZkServerAddress(),
      "--auth-conf-dir",
      dir.toAbsolutePath().toString(),
      "--solr-include-file",
      solrIncludeFile.toAbsolutePath().toString(),
      "--credentials",
      "solr:solr"
    };
    assertNotEquals(0, runTool(args, AuthTool.class));
  }

  @Test
  public void testEnableAuthAllowsUsernameEqualPasswordWithEscapeHatch() throws Exception {
    System.setProperty(Sha256AuthenticationProvider.ALLOW_USER_AS_PASSWORD_PROP, "true");
    try {
      Path solrIncludeFile = Files.createFile(dir.resolve("solrIncludeFile3.txt"));
      String[] args = {
        "auth",
        "enable",
        "-z",
        cluster.getZkClient().getZkServerAddress(),
        "--auth-conf-dir",
        dir.toAbsolutePath().toString(),
        "--solr-include-file",
        solrIncludeFile.toAbsolutePath().toString(),
        "--credentials",
        "solr:solr"
      };
      assertEquals(0, runTool(args, AuthTool.class));
    } finally {
      System.clearProperty(Sha256AuthenticationProvider.ALLOW_USER_AS_PASSWORD_PROP);
    }
  }
}

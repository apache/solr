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
package org.apache.solr.cli.tools;

import org.apache.solr.cli.CLITestHelper;
import org.apache.solr.cli.CLIUtils;
import org.apache.solr.cli.ToolBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigToolTest extends SolrCloudTestCase {
  private static final String COLLECTION = "configToolColl";

  /** Runs the tool. Overridden by the picocli variant of this test. */
  protected int runTool(String[] args, Class<? extends ToolBase> clazz) throws Exception {
    return CLITestHelper.runTool(args, clazz);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "config", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
  }

  private String overlay() throws Exception {
    try (SolrClient client =
        CLIUtils.getSolrClient(cluster.getJettySolrRunner(0).getBaseUrl().toString(), null)) {
      NamedList<Object> rsp =
          client.request(
              new GenericSolrRequest(
                  SolrRequest.METHOD.GET,
                  "/" + COLLECTION + "/config/overlay",
                  new ModifiableSolrParams()));
      return String.valueOf(rsp.get("overlay"));
    }
  }

  private String[] args(String... rest) {
    String[] common = {
      "config", "-c", COLLECTION, "-s", cluster.getJettySolrRunner(0).getBaseUrl().toString()
    };
    String[] all = new String[common.length + rest.length];
    System.arraycopy(common, 0, all, 0, common.length);
    System.arraycopy(rest, 0, all, common.length, rest.length);
    return all;
  }

  @Test
  public void testSetAndUnsetProperty() throws Exception {
    assertEquals(
        0,
        runTool(
            args("--property", "updateHandler.autoCommit.maxDocs", "--value", "100"),
            ConfigTool.class));
    assertTrue(overlay(), overlay().contains("maxDocs=100"));

    assertEquals(
        0,
        runTool(
            args("--action", "unset-property", "--property", "updateHandler.autoCommit.maxDocs"),
            ConfigTool.class));
    assertFalse(overlay(), overlay().contains("maxDocs"));
  }

  @Test
  public void testSetPropertyRequiresValue() throws Exception {
    assertEquals(
        1, runTool(args("--property", "updateHandler.autoCommit.maxDocs"), ConfigTool.class));
  }

  @Test
  public void testSetAndUnsetUserProperty() throws Exception {
    assertEquals(
        0,
        runTool(
            args("--action", "set-user-property", "--property", "my.prop", "--value", "abc"),
            ConfigTool.class));
    assertTrue(overlay(), overlay().contains("my.prop=abc"));

    assertEquals(
        0,
        runTool(
            args("--action", "unset-user-property", "--property", "my.prop"), ConfigTool.class));
    assertFalse(overlay(), overlay().contains("my.prop"));
  }

  @Test
  public void testUnknownActionFails() throws Exception {
    // commons-cli passes it on and the Config API refuses it (1); picocli rejects it first (2)
    assertNotEquals(
        0,
        runTool(
            args("--action", "no-such-action", "--property", "my.prop", "--value", "x"),
            ConfigTool.class));
    assertFalse(overlay(), overlay().contains("my.prop"));
  }
}

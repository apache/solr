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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import org.apache.solr.cli.CLITestHelper;
import org.apache.solr.cli.ToolBase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class PostLogsToolCliTest extends SolrCloudTestCase {
  private static final String COLLECTION = "postLogsToolColl";

  /** Runs the tool. Overridden by the picocli variant of this test. */
  protected int runTool(String[] args, Class<? extends ToolBase> clazz) throws Exception {
    return CLITestHelper.runTool(args, clazz);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
  }

  private String solrUrl() {
    return cluster.getJettySolrRunner(0).getBaseUrl().toString();
  }

  private Path logDirWithTwoQueries() throws Exception {
    Path dir = createTempDir();
    String record =
        "2019-12-09 15:05:%02d.931 INFO  (qtp2103763750-21) [c:logs4 s:shard1 r:core_node2 x:logs4_shard1_replica_n1] o.a.s.c.S.Request [logs4_shard1_replica_n1]  path=/select params={q=*:*&wt=javabin} hits=1 status=0 QTime=8\n";
    Files.writeString(
        dir.resolve("solr.log"),
        String.format(Locale.ROOT, record, 11) + String.format(Locale.ROOT, record, 12),
        StandardCharsets.UTF_8);
    return dir;
  }

  @Test
  public void testPostsLogRecords() throws Exception {
    String[] args = {
      "postlogs",
      "-c",
      COLLECTION,
      "--solr-url",
      solrUrl(),
      "--rootdir",
      logDirWithTwoQueries().toString()
    };
    assertEquals(0, runTool(args, PostLogsTool.class));

    long found =
        cluster.getSolrClient().query(COLLECTION, new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(2, found);
  }

  @Test
  public void testFailsWithoutAConnectionTarget() throws Exception {
    String[] args = {"postlogs", "-c", COLLECTION, "--rootdir", logDirWithTwoQueries().toString()};
    assertNotEquals(0, runTool(args, PostLogsTool.class));
  }
}

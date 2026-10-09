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
package org.apache.solr.cli.tools.snapshot;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.cli.CLITestHelper;
import org.apache.solr.cli.ToolBase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

/** The snapshot-create, -list, -describe and -delete commands through the command line. */
public class SnapshotToolsTest extends SolrCloudTestCase {
  static final String COLLECTION = "snapshotToolsColl";

  /** Runs the tool. Overridden by the picocli variant of this test. */
  protected int runTool(
          String[] args, CLITestHelper.TestingRuntime runtime, Class<? extends ToolBase> clazz)
      throws Exception {
    return CLITestHelper.runTool(args, runtime, clazz);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
    cluster.getSolrClient().add(COLLECTION, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().commit(COLLECTION);
  }

  String run(Class<? extends ToolBase> tool, String name, String... extra) throws Exception {
    List<String> args =
        new ArrayList<>(
            List.of(
                name,
                "-c",
                COLLECTION,
                "--solr-url",
                cluster.getJettySolrRunner(0).getBaseUrl().toString()));
    args.addAll(List.of(extra));
    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    assertEquals(0, runTool(args.toArray(new String[0]), runtime, tool));
    return runtime.getOutput();
  }

  @Test
  public void testSnapshotLifecycle() throws Exception {
    run(SnapshotCreateTool.class, "snapshot-create", "--snapshot-name", "snap1");

    assertTrue(run(SnapshotListTool.class, "snapshot-list").contains("snap1"));

    String described =
        run(SnapshotDescribeTool.class, "snapshot-describe", "--snapshot-name", "snap1");
    assertTrue(described, described.contains("Name: snap1"));

    run(SnapshotDeleteTool.class, "snapshot-delete", "--snapshot-name", "snap1");

    assertFalse(run(SnapshotListTool.class, "snapshot-list").contains("snap1"));
  }

  // snapshot-list reports a failed request in its output and still exits 0, so check the output.
  @Test
  public void testConnectionFallsBackToTheZkHostProperty() throws Exception {
    run(SnapshotCreateTool.class, "snapshot-create", "--snapshot-name", "snapViaProperty");

    System.setProperty("zkHost", cluster.getZkClient().getZkServerAddress());
    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    assertEquals(
        0,
        runTool(new String[] {"snapshot-list", "-c", COLLECTION}, runtime, SnapshotListTool.class));
    assertTrue(runtime.getOutput(), runtime.getOutput().contains("snapViaProperty"));
    run(SnapshotDeleteTool.class, "snapshot-delete", "--snapshot-name", "snapViaProperty");
  }
}

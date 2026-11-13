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

package org.apache.solr.shell;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of {@code test_snapshots.bats}.
 *
 * <p>{@code solr start -e films} is deliberately not used: on Windows {@code
 * RunExampleTool#startSolr} drops every {@code SOLR_*} variable from the spawned node's
 * environment, so the node carries neither the harness' process-scan marker nor {@code
 * SOLR_PID_DIR} and cannot be discovered or stopped (SOLR-18650). These tests only need a
 * collection named {@code films} with documents in it, so a plain start plus {@code solr create}
 * and {@code solr post} stand in.
 *
 * <p>Every snapshot a test creates it also deletes, and no two tests share a snapshot name: {@code
 * SOLR_HOME} is wiped per class rather than per test, so a failure between a create and its delete
 * would otherwise resurface as a second failure in the sibling test that reused the name.
 */
public class SnapshotsTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // The node lives for the whole class and is stopped automatically.
    classCli.run("solr start");
    classCli.run("solr create -c films -d _default");
    // Documents, so a snapshot references real segment files rather than an empty commit. The
    // original got them from 'solr start -e films'; these are the same ones PostTest indexes.
    classCli
        .run(
            "solr post --filetypes xml,json,csv --solr-url http://localhost:${SOLR_PORT}"
                + " -c films \"${SOLR_TIP}/example/exampledocs\"")
        .assertNotContains("ERROR");
    classCli.helpers().http("/solr/films/select?q=*:*").assertContains("\"numFound\":45");
  }

  @Test
  public void snapshotLifecycle() {
    // the way snapshots live, if you run create twice you get an error because snapshot already
    // exists
    run("solr snapshot-create -c films --snapshot-name snapshot1 --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Successfully created snapshot with name snapshot1 for collection films");

    run("solr snapshot-delete -c films --snapshot-name snapshot1 --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Successfully deleted snapshot with name snapshot1 for collection films");

    // make sure you can create it again!
    run("solr snapshot-create -c films --snapshot-name snapshot1 -z localhost:${ZK_PORT}")
        .assertContains("Successfully created snapshot with name snapshot1 for collection films");

    run("solr snapshot-delete -c films --snapshot-name snapshot1")
        .assertContains("Successfully deleted snapshot with name snapshot1 for collection films");
  }

  @Test
  public void snapshotList() {
    run(
        "solr snapshot-create -c films --snapshot-name snapshot3 --solr-url http://localhost:${SOLR_PORT}");

    // Confirm that we continue to normalize away the /solr ending.
    run("solr snapshot-list -c films -s http://localhost:${SOLR_PORT}/solr")
        .assertContains("snapshot3");

    run("solr snapshot-delete -c films --snapshot-name snapshot3 -s http://localhost:${SOLR_PORT}")
        .assertContains("Successfully deleted snapshot with name snapshot3 for collection films");
  }

  @Test
  public void snapshotListViaSolrConnection() {
    run(
        "solr snapshot-create -c films --snapshot-name snapshot3_conn --solr-connection http://localhost:${SOLR_PORT}/solr");

    // Confirm that we continue to normalize away the /solr ending.
    run("solr snapshot-list -c films --solr-connection http://localhost:${SOLR_PORT}/solr")
        .assertContains("snapshot3_conn");

    run("solr snapshot-delete -c films --snapshot-name snapshot3_conn --solr-connection http://localhost:${SOLR_PORT}/solr")
        .assertContains(
            "Successfully deleted snapshot with name snapshot3_conn for collection films");
  }

  @Test
  public void snapshotDescribe() {
    run("solr snapshot-create -c films --snapshot-name snapshot4 -s http://localhost:${SOLR_PORT}");

    run("solr snapshot-describe -c films --snapshot-name snapshot4")
        .assertContains("Name: snapshot4");

    run("solr snapshot-delete -c films --snapshot-name snapshot4")
        .assertContains("Successfully deleted snapshot with name snapshot4 for collection films");
  }

  @Test
  public void snapshotDescribeViaSolrConnection() {
    run(
        "solr snapshot-create -c films --snapshot-name snapshot4_conn --solr-connection http://localhost:${SOLR_PORT}/solr");

    run("solr snapshot-describe -c films --snapshot-name snapshot4_conn --solr-connection http://localhost:${SOLR_PORT}/solr")
        .assertContains("Name: snapshot4_conn");

    run("solr snapshot-delete -c films --snapshot-name snapshot4_conn --solr-connection http://localhost:${SOLR_PORT}/solr")
        .assertContains(
            "Successfully deleted snapshot with name snapshot4_conn for collection films");
  }
}

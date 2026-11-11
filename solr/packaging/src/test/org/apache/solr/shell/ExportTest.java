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

import org.apache.solr.shell.exec.Shell;
import org.junit.Test;

/**
 * Port of {@code test_export.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs: {@code ${BATS_TEST_TMPDIR}} becomes an {@code out} directory in the session's
 * own work directory (which is also its cwd, so the relative-path cases read as they did in bash);
 * {@code [ -e ... ]} becomes {@link #exists}; each {@code solr export} also asserts its exit
 * status, since a file-existence check alone would pass on a leftover file; and each test deletes
 * the collection it created, because the harness wipes {@code SOLR_HOME} once per class rather than
 * before every test and a stale core directory would be discovered by the other test's node.
 */
public class ExportTest extends SolrShellTestCase {

  @Test
  @EnabledOnShell(Shell.BASH)
  // RunExampleTool#startSolr drops every SOLR_* variable from the environment of the node it spawns
  // on Windows, so the example's node would get neither the harness' process-scan marker nor
  // SOLR_PID_DIR, and could not be discovered or stopped afterwards.
  public void checkExportCommand() {
    // --solr-home is not in the BATS original, and is not optional. RunExampleTool reads the
    // property solr.home (RunExampleTool:597), while the SOLR_HOME environment variable maps to
    // solr.solr.home (EnvToSyspropMappings.properties:15) — so the variable never reaches it and
    // the example falls back to serverDir/solr, inside the distribution every class shares.
    run("solr start -e techproducts --solr-home \"${SOLR_HOME}\"").assertSuccess();

    // Stands in for ${BATS_TEST_TMPDIR}: the --output cases need both a file prefix and a real
    // directory below the session's work directory.
    mkdirs("out");

    run("solr export --solr-url http://localhost:${SOLR_PORT} --name techproducts"
            + " --query \"*:* -id:test\" --output \"out/output\"")
        .assertSuccess()
        .assertNotContains("Unrecognized option")
        .assertContains("Export complete");

    assertTrue(exists("out/output.json"));

    run("solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\"")
        .assertSuccess();
    assertTrue(exists("techproducts.json"));
    rm("techproducts.json");

    run("solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --format javabin")
        .assertSuccess();
    assertTrue(exists("techproducts.javabin"));
    rm("techproducts.javabin");

    // Removed first so the existence check proves this export wrote the file, rather than passing
    // on the one the first export above left behind.
    rm("out/output.json");
    run("solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --output \"out/output.json\"")
        .assertSuccess();
    assertTrue(exists("out/output.json"));

    run("solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --output \"out\"")
        .assertSuccess();
    assertTrue(exists("out/techproducts.json"));

    run("solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --format jsonl --output \"out/output\"")
        .assertSuccess();
    assertTrue(exists("out/output.jsonl"));

    run("solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --limit 10 --compress --format jsonl"
            + " --output \"out/output\"")
        .assertSuccess()
        .assertContains("Total Docs exported: 10");
    assertTrue(exists("out/output.jsonl.gz"));

    // SOLR_HOME lives for the whole class, so leave no techproducts core behind for the
    // user-managed node of the next test to discover.
    run("solr delete -c techproducts --delete-config").assertSuccess();
  }

  @Test
  public void exportFailsOnNonCloudMode() {
    run("solr start --user-managed").assertSuccess();
    run("solr create -c COLL_NAME").assertSuccess();
    // Strengthened: "fails" is part of the claim, and the message check alone would pass even if
    // the command had done nothing.
    run("solr export --solr-url http://localhost:${SOLR_PORT} -c COLL_NAME")
        .assertFailure()
        .assertNotContains("Export complete");

    // As above: don't leave a user-managed core in the class' SOLR_HOME.
    run("solr delete -c COLL_NAME").assertSuccess();
  }
}

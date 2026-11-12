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
 *
 * <p>Both tests build their own node, and neither uses {@code solr start -e}, so both run on
 * cmd.exe as well — see the comment at the fixture.
 */
public class ExportTest extends SolrShellTestCase {

  @Test
  public void checkExportCommand() {
    // The original built this fixture with 'solr start -e techproducts', which would make the test
    // bash-only (see SnapshotsTest). The subject here is 'solr export', so the fixture is built the
    // long way instead; ExampleTest is where 'solr start -e' is covered deliberately.
    run("solr start");
    run("solr create -c techproducts -d sample_techproducts_configs");
    run("solr post --type application/xml --filetypes xml --solr-url http://localhost:${SOLR_PORT}"
            + " -c techproducts \"${SOLR_TIP}/example/exampledocs\"")
        .assertNotContains("ERROR");

    // Stands in for ${BATS_TEST_TMPDIR}: the --output cases need both a file prefix and a real
    // directory below the session's work directory.
    mkdirs("out");

    run("solr export --solr-url http://localhost:${SOLR_PORT} --name techproducts"
            + " --query \"*:* -id:test\" --output \"out/output\"")
        .assertNotContains("Unrecognized option")
        .assertContains("Export complete");

    assertTrue(exists("out/output.json"));

    run(
        "solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\"");
    assertTrue(exists("techproducts.json"));
    rm("techproducts.json");

    run(
        "solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --format javabin");
    assertTrue(exists("techproducts.javabin"));
    rm("techproducts.javabin");

    // Removed first so the existence check proves this export wrote the file, rather than passing
    // on the one the first export above left behind.
    rm("out/output.json");
    run(
        "solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --output \"out/output.json\"");
    assertTrue(exists("out/output.json"));

    run(
        "solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --output \"out\"");
    assertTrue(exists("out/techproducts.json"));

    run(
        "solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --format jsonl --output \"out/output\"");
    assertTrue(exists("out/output.jsonl"));

    run("solr export --solr-url http://localhost:${SOLR_PORT} -c techproducts"
            + " --query \"*:* -id:test\" --limit 10 --compress --format jsonl"
            + " --output \"out/output\"")
        .assertContains("Total Docs exported: 10");
    assertTrue(exists("out/output.jsonl.gz"));

    // SOLR_HOME lives for the whole class, so leave no techproducts core behind for the
    // user-managed node of the next test to discover.
    run("solr delete -c techproducts --delete-config");
  }

  @Test
  public void exportFailsOnNonCloudMode() {
    run("solr start --user-managed");
    run("solr create -c COLL_NAME");
    // Strengthened: "fails" is part of the claim, and the message check alone would pass even if
    // the command had done nothing.
    runFail("solr export --solr-url http://localhost:${SOLR_PORT} -c COLL_NAME")
        .assertNotContains("Export complete");

    // As above: don't leave a user-managed core in the class' SOLR_HOME.
    run("solr delete -c COLL_NAME");
  }
}

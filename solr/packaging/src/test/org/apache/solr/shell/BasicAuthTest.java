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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Port of {@code test_basic_auth.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs:
 *
 * <ul>
 *   <li>The original's {@code setup()} ran before every test; the harness wipes {@code SOLR_HOME}
 *       once per class, so {@link #startSolrWithBasicAuth} does the rest. Each test therefore gets
 *       its own node, its own embedded ZooKeeper data and its own {@code security.json}, which is
 *       what lets two tests share the collection name {@code COLL_NAME}.
 *   <li>The one-command {@code SOLR_INCLUDE=/force/credentials/to/be/supplied} prefix becomes an
 *       {@code env()} that stays set for the test: the harness environment is per-session, not
 *       per-command, and the path is one inside the work directory rather than an absolute Unix
 *       one. Keeping it set for the whole test also keeps {@code solr auth disable} away from the
 *       distribution's own include file, which every test class shares.
 *   <li>{@code curl -I} becomes {@link #http}: its status is the original's {@code "HTTP/1.1 200"},
 *       and the header assertion is lower-cased because {@code java.net.http} lower-cases header
 *       names.
 *   <li>The second test's credential-less {@code run solr create -c COLL_NAME}, which the original
 *       leaves unasserted, asserts that it is refused.
 * </ul>
 */
public class BasicAuthTest extends SolrShellTestCase {

  @Before
  public void startSolrWithBasicAuth() throws IOException {
    // BATS' common_clean_setup: every test in this file starts its own Solr from a clean SOLR_HOME.
    IOUtils.rm(harnessEnv.solrHome());
    Files.createDirectories(harnessEnv.logsDir());

    run("solr start -Dsolr.packages.enabled=true").assertSuccess();

    // The auth command exports some system variables that are injected as basic auth username and
    // password, however that defeats our test so fake that out by pointing SOLR_INCLUDE at a path
    // that does not exist: 'auth enable' then writes neither the include file nor
    // SOLR_HOME/basicAuth.conf, and every command below has to supply the credentials itself.
    Path bogusInclude = workDir().resolve("force-credentials-to-be-supplied").resolve("solr.in.sh");
    env("SOLR_INCLUDE", bogusInclude.toString());
    run("solr auth enable --type basicAuth --credentials name:password").assertSuccess();

    run("solr assert --credentials name:password --cloud http://localhost:${SOLR_PORT}"
            + " --timeout 5000")
        .assertSuccess();
  }

  @After
  public void disableBasicAuth() {
    // BATS' teardown. Stopping the node is automatic; SOLR_INCLUDE still points at the path that
    // does not exist, so this only clears security.json out of ZooKeeper.
    run("solr auth disable -z localhost:${ZK_PORT}");
  }

  // Remaining commands that should support basic auth:
  // package, export

  @Test
  public void createConfigApiAndDeleteWithBasicAuth() {
    // Test create
    run("solr create -u name:password -c COLL_NAME")
        .assertContains("Created collection 'COLL_NAME'");

    // Test config
    run("solr config -u name:password -c COLL_NAME --action set-property --property"
            + " updateHandler.autoCommit.maxDocs --value 100 --solr-url"
            + " http://localhost:${SOLR_PORT}/solr")
        .assertContains("Successfully set-property updateHandler.autoCommit.maxDocs to 100");

    // Test api
    run("solr api -u name:password --solr-url"
            + " \"http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=*:*\" --verbose")
        .assertContains("\"numFound\":0");

    // Test delete
    run("solr delete --credentials name:password -c COLL_NAME -z localhost:${ZK_PORT}"
            + " --delete-config --verbose")
        .assertContains("Deleted collection 'COLL_NAME'");
    assertFalse(collectionExists("COLL_NAME"));
    assertFalse(configExists("COLL_NAME"));
  }

  @Test
  public void postPostlogsAndExportWithBasicAuth() {
    // Strengthened: the original ignores this line's outcome, but without credentials the create
    // has to be refused.
    run("solr create -c COLL_NAME").assertFailure();
    run("solr create -u name:password -c COLL_NAME")
        .assertContains("Created collection 'COLL_NAME'");

    // Test post
    run("solr post -u name:password -t application/xml --solr-url"
            + " http://localhost:${SOLR_PORT} --name monitors"
            + " ${SOLR_TIP}/example/exampledocs/monitor.xml")
        .assertContains("1 files indexed.");

    // Test postlogs
    run("solr postlogs -u name:password --solr-url http://localhost:${SOLR_PORT}"
            + " --name COLL_NAME --rootdir ${SOLR_LOGS_DIR}/solr.log")
        .assertContains("Committed");

    // Test export (commented out in the original too)
    // run("solr export -u name:password --solr-url http://localhost:${SOLR_PORT}"
    //         + " --name COLL_NAME --query \"*:*\" --output \"export-output\"")
    //     .assertContains("Export complete");
  }

  @Test
  public void packageWithBasicAuth() {
    run("solr package deploy PACKAGE_NAME --credentials name:password --collections foo-1.2")
        // verify that package tool is communicating with Solr via basic auth
        .assertContains("Collection(s) doesn't exist: [foo-1.2]");
  }

  @Test
  public void adminUiStaticContentIsExceptedFromBasicAuth() {
    http("/solr/").assertStatus(200).assertHeaderContains("content-type: text/html");
  }
}

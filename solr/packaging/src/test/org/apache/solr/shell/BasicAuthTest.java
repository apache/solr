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
 * <p>{@code SOLR_INCLUDE} points at a non-existent path inside the work directory, so {@code auth
 * enable} writes neither an include file nor {@code basicAuth.conf} and every command has to supply
 * the credentials itself. That also keeps {@code solr auth disable} away from the distribution's
 * own include file, which every test class shares.
 */
public class BasicAuthTest extends SolrShellTestCase {

  @Before
  public void startSolrWithBasicAuth() throws IOException {
    // Every test starts its own Solr from a clean SOLR_HOME.
    IOUtils.rm(harnessEnv.solrHome());
    Files.createDirectories(harnessEnv.logsDir());

    run("solr start -Dsolr.packages.enabled=true");

    // The auth command exports some system variables that are injected as basic auth username and
    // password, however that defeats our test so fake that out by pointing SOLR_INCLUDE at a path
    // that does not exist: 'auth enable' then writes neither the include file nor
    // SOLR_HOME/basicAuth.conf, and every command below has to supply the credentials itself.
    Path bogusInclude = workDir().resolve("force-credentials-to-be-supplied").resolve("solr.in.sh");
    env("SOLR_INCLUDE", bogusInclude.toString());
    run("solr auth enable --type basicAuth --credentials name:password");

    run(
        "solr assert --credentials name:password --cloud http://localhost:${SOLR_PORT}"
            + " --timeout 5000");
  }

  @After
  public void disableBasicAuth() {
    // Stopping the node is automatic; SOLR_INCLUDE still points at the path that
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
    // Without credentials the create
    // has to be refused.
    runFail("solr create -c COLL_NAME");
    run("solr create -u name:password -c COLL_NAME")
        .assertContains("Created collection 'COLL_NAME'");

    // Test post
    // Posts to the collection this test created. Naming a collection nothing creates makes every
    // document 404 while `solr post` still prints "1 files indexed." - recorded separately.
    run("solr post -u name:password -t application/xml --solr-url"
            + " http://localhost:${SOLR_PORT} --name COLL_NAME"
            + " \"${SOLR_TIP}/example/exampledocs/monitor.xml\"")
        .assertContains("1 files indexed.");

    // Test postlogs
    run("solr postlogs -u name:password --solr-url http://localhost:${SOLR_PORT}"
            + " --name COLL_NAME --rootdir \"${SOLR_LOGS_DIR}/solr.log\"")
        .assertContains("Committed");

    // Test export (disabled)
    // run("solr export -u name:password --solr-url http://localhost:${SOLR_PORT}"
    //         + " --name COLL_NAME --query \"*:*\" --output \"export-output\"")
    //     .assertContains("Export complete");
  }

  @Test
  public void packageWithBasicAuth() {
    runFail("solr package deploy PACKAGE_NAME --credentials name:password --collections foo-1.2")
        // verify that package tool is communicating with Solr via basic auth
        .assertContains("Collection(s) doesn't exist: [foo-1.2]");
  }

  @Test
  public void adminUiStaticContentIsExceptedFromBasicAuth() {
    http("/solr/").assertStatus(200).assertHeaderContains("content-type: text/html");
  }
}

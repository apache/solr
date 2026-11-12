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
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.shell.exec.Shell;
import org.junit.Before;
import org.junit.Test;

/**
 * Port of {@code test_auth.bats}. There are further auth tests in {@link SslTest}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs:
 *
 * <ul>
 *   <li>{@code curl} becomes {@link #http}, which now also carries credentials, {@code curl -u
 *       user:password --basic} being half of what this suite does.
 *   <li>{@code solr auth enable} rewrites the include file, by default the distribution's own and
 *       so shared with every concurrent class, so {@link #cleanSetup} points {@code SOLR_INCLUDE}
 *       at a private copy — as {@code SslTest} does.
 *   <li>The original's {@code setup} runs {@code solr auth disable} before every test, to undo what
 *       a previous test left in the include file and in ZooKeeper. Here the include file is a
 *       pristine copy per test and {@code SOLR_HOME} — which holds the embedded ZooKeeper's data —
 *       is wiped per test, so there is nothing to disable.
 *   <li>The two {@code auth enable} calls the original lets succeed-or-fail silently are asserted
 *       to succeed.
 * </ul>
 */
public class AuthTest extends SolrShellTestCase {

  @Before
  public void cleanSetup() throws IOException {
    // BATS' common_clean_setup: every test here starts its own Solr, from a clean SOLR_HOME. The
    // wipe also discards the embedded ZooKeeper's data, and with it any security.json.
    IOUtils.rm(harnessEnv.solrHome());
    Files.createDirectories(harnessEnv.logsDir());

    // 'solr auth enable' appends to the include file, which by default belongs to the distribution
    // every test class shares, so each test gets a private copy in its own work directory. The
    // name has to keep its extension: bin\solr.cmd CALLs the include file.
    String includeFile = cli.shell() == Shell.BASH ? "solr.in.sh" : "solr.in.cmd";
    env(
        "SOLR_INCLUDE",
        copy(harnessEnv.solrTip().resolve("bin").resolve(includeFile).toString(), includeFile)
            .toString());
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18648")
  public void authRejectsBlockUnknownOptionWithInvalidBoolean() {
    runFail("solr auth enable --type basicAuth --credentials any:any --block-unknown ture")
        .assertContains("Argument [block-unknown] must be either true or false, but was [ture]");
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18648")
  public void authRejectsUpdateIncludeFileOnlyOptionWithInvalidBoolean() {
    runFail(
            "solr auth enable --type basicAuth --credentials any:any --update-include-file-only ture")
        .assertContains(
            "Argument [update-include-file-only] must be either true or false, but was [ture]");
  }

  @Test
  public void authEnableDisableLifecycle() {
    run("solr start");
    run("solr auth enable --type basicAuth --credentials name:password");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");

    http(
            "/solr/admin/collections?action=CREATE&collection.configName=_default&name=test"
                + "&numShards=2&replicationFactor=1&router.name=compositeId&wt=json",
            "name",
            "password")
        .assertContains("\"status\":0");

    run("solr auth disable");
    http("/solr/test/select?q=*:*").assertContains("\"numFound\":0");
    run("solr stop --all");
  }

  @Test
  public void enableAuthConnectsViaZookeeper() {
    run("solr start");
    run("solr auth enable --type basicAuth --credentials name:password -z localhost:${ZK_PORT}")
        .assertContains("Successfully enabled basic auth");
    http("/api/collections", "name", "password").assertContains("\"status\":0");

    run("solr auth disable -z localhost:${ZK_PORT}");
    http("/api/collections").assertContains("\"status\":0");
    run("solr stop --all");
  }
}

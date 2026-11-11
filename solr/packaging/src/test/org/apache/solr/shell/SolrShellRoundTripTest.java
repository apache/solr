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

import java.time.Duration;
import java.util.List;
import org.apache.solr.shell.exec.Shell;
import org.junit.Test;

/**
 * The first end-to-end exercise of the shell harness: start Solr, create a collection, index a
 * handful of documents, search them back, and stop again — all through {@code bin/solr}.
 */
public class SolrShellRoundTripTest extends SolrShellTestCase {

  private static final String COLLECTION = "books";

  @Test
  public void startCreateIndexSearchStop() {
    run("solr start").assertSuccess();
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000").assertSuccess();

    bind("collection", COLLECTION);
    run("solr create -c {{collection}}")
        .assertSuccess()
        .assertOutputContains("Created collection '{{collection}}'");
    assertTrue("collection should exist in ZooKeeper", collectionExists(COLLECTION));

    run("""
        solr post -c {{collection}} "${SOLR_TIP}/example/exampledocs/books.json"
        """)
        .assertSuccess();

    // Searching twice over, once through the CLI and once through the harness' own HTTP helper,
    // because the BATS suites reach for both 'solr api' and curl. The argv form keeps the URL's
    // quotes, '?' and '&' out of the command line entirely — the harness quotes each argument for
    // the shell in use.
    run(List.of(
            "solr",
            "api",
            "--solr-url",
            "http://localhost:${SOLR_PORT}/solr/" + COLLECTION + "/select?q=*:*&rows=0"))
        .assertSuccess()
        .assertOutputContains("\"numFound\":4");

    var response = http("/solr/" + COLLECTION + "/select?q=name:Monsters");
    assertEquals("search should return HTTP 200", 200, response.status());
    assertTrue(
        "search should find The Sea of Monsters, got: " + response.body(),
        response.body().contains("The Sea of Monsters"));

    run("solr stop -p ${SOLR_PORT}").assertSuccess();
    awaitOrFail(
        "Solr on port " + ports().solr() + " to stop",
        Duration.ofSeconds(30),
        Duration.ofSeconds(1),
        () ->
            run("solr assert --not-started http://localhost:${SOLR_PORT} --timeout 5000")
                .succeeded());
  }

  @Test
  @EnabledOnShell(Shell.BASH)
  public void bashOnlyTestsRunUnderBash() {
    assertEquals(Shell.BASH, cli.shell());
  }

  @Test
  @EnabledOnCli(Cli.COMMONS_CLI)
  public void commonsCliOnlyTestsSeeCommonsCli() {
    assertEquals(Cli.COMMONS_CLI, cli());
  }

  @Test
  @EnabledOnCli(Cli.PICOCLI)
  public void picocliOnlyTestsSeePicocli() {
    assertEquals(Cli.PICOCLI, cli());
  }

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdOnlyTestsAreSkippedUnderBash() {
    // The gate's own proof: under bash this must not execute, so the suite only stays green if
    // @EnabledOnShell skipped it. Under cmd.exe it is the assertion that has to hold.
    assertEquals(Shell.CMD, cli.shell());
  }
}

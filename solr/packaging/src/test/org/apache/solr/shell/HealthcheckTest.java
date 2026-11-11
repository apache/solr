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
 * Port of {@code test_healthcheck.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on. The
 * original starts Solr inside each test rather than in {@code setup_file} — one node in cloud mode,
 * one user-managed — so each test here starts its own too; the harness stops it when the test ends,
 * which is the original's {@code teardown}. Where it differs: BATS wiped {@code SOLR_HOME} before
 * every test, while here the whole class shares one, so the cloud test deletes its collection
 * again; and the two {@code refute_output 'error'} checks also assert the exit status, which an
 * output-only refutation would not notice.
 */
public class HealthcheckTest extends SolrShellTestCase {

  @Test
  public void healthcheckOnCloudSolr() {
    run("solr start").assertSuccess();
    run("solr create -c healthcheck_test -d _default").assertSuccess();

    // Local. Strengthened: the original only refuted "error", which an empty output satisfies
    // without the healthcheck having run.
    run("solr healthcheck -c healthcheck_test").assertSuccess().assertNotContains("error");

    // Remote
    run("solr healthcheck -c healthcheck_test --solr-connection http://localhost:${SOLR_PORT}/solr")
        .assertSuccess()
        .assertNotContains("error");

    // Not in the original: SOLR_HOME is class-scoped here, so the collection has to go again
    // before the user-managed test starts a node over the same home.
    run("solr delete -c healthcheck_test").assertSuccess();
  }

  @Test
  public void healthcheckErrorsOnStandaloneSolr() {
    run("solr start --user-managed").assertSuccess();
    // Strengthened: the test's claim is that it errors, so the status is part of it.
    run("solr healthcheck -c healthcheck_test")
        .assertFailure()
        .assertContains("Healthcheck tool only works in Solr Cloud mode");
  }
}

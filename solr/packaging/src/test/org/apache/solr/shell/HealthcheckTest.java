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
 * <p>Each test starts its own node — one in cloud mode, one user-managed — and the cloud test
 * deletes its collection again, the class sharing one {@code SOLR_HOME}.
 */
public class HealthcheckTest extends SolrShellTestCase {

  @Test
  public void healthcheckOnCloudSolr() {
    run("solr start");
    run("solr create -c healthcheck_test -d _default");

    // Local. An empty output would satisfy a bare notContains("error").
    run("solr healthcheck -c healthcheck_test").assertNotContains("error");

    // Remote
    run("solr healthcheck -c healthcheck_test --solr-connection http://localhost:${SOLR_PORT}/solr")
        .assertNotContains("error");

    // SOLR_HOME is class-scoped, so the collection has to go again before the user-managed test
    // starts a node over the same home.
    run("solr delete -c healthcheck_test");
  }

  @Test
  public void healthcheckErrorsOnStandaloneSolr() {
    run("solr start --user-managed");
    // The test's claim is that it errors, so the status is part of it.
    runFail("solr healthcheck -c healthcheck_test")
        .assertContains("Healthcheck tool only works in Solr Cloud mode");
  }
}

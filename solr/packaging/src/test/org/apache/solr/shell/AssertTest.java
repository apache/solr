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
 * Port of {@code test_assert.bats}.
 *
 * <p>Each test starts its own node — one user-managed, one in cloud mode — and the harness stops it
 * when the test ends.
 */
public class AssertTest extends SolrCatsTestCase {

  @Test
  public void assertForNonCloudMode() {
    run("solr assert --not-started http://localhost:${SOLR_PORT} --timeout 5000");
    run("solr start --user-managed");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");

    run("solr assert --not-cloud http://localhost:${SOLR_PORT}/solr")
        .assertContains("needn't include Solr's context-root")
        .assertNotContains("ERROR");

    runFail("solr assert --cloud http://localhost:${SOLR_PORT}")
        .assertContains("ERROR: Solr is not running in cloud mode");

    runFail("solr assert --cloud http://localhost:${SOLR_PORT} --exitcode");
  }

  @Test
  public void assertForCloudMode() {
    run("solr start");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");

    // An empty output would satisfy a bare notContains("ERROR"), so the status is asserted too.
    run("solr assert --cloud http://localhost:${SOLR_PORT}").assertNotContains("ERROR");

    runFail("solr assert --not-cloud http://localhost:${SOLR_PORT}/solr")
        .assertContains("needn't include Solr's context-root")
        .assertContains("ERROR: Solr is not running in standalone mode");

    runFail("solr assert --not-cloud http://localhost:${SOLR_PORT} --exitcode");
  }
}

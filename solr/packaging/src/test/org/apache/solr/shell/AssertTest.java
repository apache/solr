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
 * <p>Kept close to the original, including the order of the tests and the text each asserts on. The
 * original starts Solr inside each test rather than in {@code setup_file} — one node user-managed,
 * one in cloud mode — so each test here starts its own too; the harness stops it when the test
 * ends, which is the original's {@code teardown}.
 */
public class AssertTest extends SolrShellTestCase {

  @Test
  public void assertForNonCloudMode() {
    run("solr assert --not-started http://localhost:${SOLR_PORT} --timeout 5000").assertSuccess();
    run("solr start --user-managed");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000").assertSuccess();

    run("solr assert --not-cloud http://localhost:${SOLR_PORT}/solr")
        .assertContains("needn't include Solr's context-root")
        .assertNotContains("ERROR");

    run("solr assert --cloud http://localhost:${SOLR_PORT}")
        .assertContains("ERROR: Solr is not running in cloud mode");

    run("solr assert --cloud http://localhost:${SOLR_PORT} --exitcode").assertFailure();
  }

  @Test
  public void assertForCloudMode() {
    run("solr start");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000").assertSuccess();

    // Strengthened: the original only refuted "ERROR", which an empty output satisfies without
    // the assert having done anything.
    run("solr assert --cloud http://localhost:${SOLR_PORT}")
        .assertSuccess()
        .assertNotContains("ERROR");

    run("solr assert --not-cloud http://localhost:${SOLR_PORT}/solr")
        .assertContains("needn't include Solr's context-root")
        .assertContains("ERROR: Solr is not running in standalone mode");

    run("solr assert --not-cloud http://localhost:${SOLR_PORT} --exitcode").assertFailure();
  }
}

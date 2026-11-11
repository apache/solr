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
 * Two nodes in one class, which exercises {@code SOLR2_PORT} and the harness' per-class port block.
 *
 * <p>Running alongside {@link RoundTripTest} in another fork is also the check that the per-class
 * {@code SOLR_HOME} and port block really do isolate classes: both classes start Solr, and either
 * would fail if they shared a port, a PID directory or a home.
 */
public class TwoNodeTest extends SolrShellTestCase {

  @Test
  public void twoNodesGetSeparatePortsAndStopIndependently() {
    run("solr start").assertSuccess();
    run("solr start -p ${SOLR2_PORT}").assertSuccess();

    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000").assertSuccess();
    run("solr assert --started http://localhost:${SOLR2_PORT} --timeout 5000").assertSuccess();

    // Both joined the same embedded ZooKeeper, which lives at SOLR_PORT + 1000.
    run("solr assert --cloud http://localhost:${SOLR_PORT} --timeout 5000").assertSuccess();
    run("solr zk ls / -z localhost:${ZK_PORT}").assertSuccess().assertContains("live_nodes");

    run("solr stop -p ${SOLR2_PORT}").assertSuccess();
    run("solr assert --not-started http://localhost:${SOLR2_PORT} --timeout 5000").assertSuccess();
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000").assertSuccess();
  }

  @Test
  public void statusFindsThisClassesNode() {
    run("solr start").assertSuccess();
    // NB: bare 'solr status' is the one command that scans the whole machine's process list
    // (StatusTool -> SolrProcessManager -> ProcessHandle.allProcesses), so under parallel forks it
    // also lists other classes' nodes. Only a "contains our port" assertion is safe here; an
    // exhaustive or negative one would be a race. Use 'status -p' when the scope matters.
    run("solr status").assertSuccess().assertContains("${SOLR_PORT}");
    run("solr status -p ${SOLR_PORT}")
        .assertSuccess()
        .assertContains("running on port ${SOLR_PORT}");
  }
}

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

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of {@code test_status.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Nothing in the original needs a shell construct, so the port runs on both shells. Where it
 * differs: the multiple-connection-options case also asserts the non-zero exit that "prevented"
 * implies, which the original left unchecked.
 */
public class StatusTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // BATS' setup_file. The node lives for the whole class and is stopped automatically.
    classCli.run("solr start").assertSuccess();
  }

  @Test
  public void statusDetectsLocallyRunningSolr() {
    run("solr status").assertContains("running on port ${SOLR_PORT}");
  }

  @Test
  public void statusWithSolrUrlFromUser() {
    run("solr status --solr-url http://localhost:${SOLR_PORT}").assertContains("\"solr_home\":");
  }

  @Test
  public void statusWithPortFromUser() {
    run("solr status --port ${SOLR_PORT}").assertContains("running on port ${SOLR_PORT}");
  }

  @Test
  public void multipleConnectionOptionsArePrevented() {
    // Strengthened: "prevented" means the command refuses, so the status is part of the claim.
    run("solr status --port ${SOLR_PORT} --solr-url http://localhost:${SOLR_PORT}")
        .assertFailure()
        .assertMatches("(-s|solr-url).*(mutually exclusive|already been selected)");
  }

  @Test
  public void statusWithInvalidSolrUrlFromUser() {
    run("solr status --solr-url http://invalidhost:${SOLR_PORT}")
        .assertContains("Solr at http://invalidhost:${SOLR_PORT} not online");
  }

  @Test
  public void statusWithShortFormat() {
    run("solr status --port ${SOLR_PORT} --short")
        .assertContains("http://localhost:${SOLR_PORT}/solr");
  }
}

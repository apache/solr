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
 * Port of {@code test_create.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs: BATS wiped {@code SOLR_HOME} before every test, while here the whole class
 * shares one, so the user-managed test deletes the core it created again — otherwise a stale {@code
 * COLL_NAME} core directory would be lying in {@code SOLR_HOME} when the cloud-mode test creates a
 * collection of that name.
 */
public class CreateTest extends SolrShellTestCase {

  @Test
  public void createForNonCloudMode() {
    run("solr start --user-managed");
    run("solr create -c COLL_NAME").assertContains("Created new core 'COLL_NAME'");

    // Not in the original: SOLR_HOME is class-scoped here, so the core has to go again.
    run("solr delete -c COLL_NAME").assertSuccess();
  }

  @Test
  public void createForCloudMode() {
    run("solr start");
    run("solr create -c COLL_NAME").assertContains("Created collection 'COLL_NAME'");
  }

  @Test
  public void multipleConnectionOptionsArePrevented() {
    run("solr create -c COLL_NAME2 --solr-url http://localhost:${SOLR_PORT}"
            + " -z localhost:${ZK_PORT}")
        .assertMatches("(mutually exclusive|already been selected).*(--solr-url|--zk-host|-z|-s)");
  }
}

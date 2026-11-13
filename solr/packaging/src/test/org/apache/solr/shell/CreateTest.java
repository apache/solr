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
 * <p>Each test names its own core or collection, and the user-managed test deletes the core it
 * created: the class shares one {@code SOLR_HOME}, and a core directory left behind is discovered
 * by the other test's node and fails to load.
 */
public class CreateTest extends SolrShellTestCase {

  @Test
  public void createForNonCloudMode() {
    run("solr start --user-managed");
    run("solr create -c core_non_cloud").assertContains("Created new core 'core_non_cloud'");

    // SOLR_HOME is class-scoped, so the core has to go again, or the cloud-mode test's node
    // discovers a user-managed core directory it cannot load.
    run("solr delete -c core_non_cloud");
  }

  @Test
  public void createForCloudMode() {
    run("solr start");
    run("solr create -c coll_cloud").assertContains("Created collection 'coll_cloud'");
  }

  @Test
  public void multipleConnectionOptionsArePrevented() {
    runFail(
            "solr create -c COLL_NAME2 --solr-url http://localhost:${SOLR_PORT}"
                + " -z localhost:${ZK_PORT}")
        .assertMatches("(mutually exclusive|already been selected).*(--solr-url|--zk-host|-z|-s)");
  }
}

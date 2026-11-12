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
 * Port of {@code test_postlogs.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs: {@code curl} becomes {@link #http}, and that request also asserts a 200, since
 * {@code refute_output --partial '"numFound":0'} is satisfied by any error response.
 */
public class PostlogsTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // BATS' setup_file. The node lives for the whole class and is stopped automatically, and its
    // own solr.log under ${SOLR_LOGS_DIR} is what the test indexes.
    classCli.run("solr start");
  }

  @Test
  public void postSolrLogIntoSolrViaCli() {
    run("solr create -c COLL_NAME").assertContains("Created collection 'COLL_NAME'");

    run("solr postlogs --solr-url http://localhost:${SOLR_PORT} -c COLL_NAME"
            + " --rootdir \"${SOLR_LOGS_DIR}/solr.log\"")
        .assertContains("Sending last batch")
        .assertContains("Committed");

    http("/solr/COLL_NAME/select?q=*:*").assertStatus(200).assertNotContains("\"numFound\":0");
  }
}

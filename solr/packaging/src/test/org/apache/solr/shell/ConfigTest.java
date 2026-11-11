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
 * Port of {@code test_config.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Nothing in the original needs a shell construct, so the port runs on both shells. Where it
 * differs: the original leaned on a {@code delete_all_collections} teardown to reuse one collection
 * name, while here the class shares one {@code SOLR_HOME} and ZooKeeper, so each test owns a
 * distinct collection; and the missing-{@code --value} case also asserts the non-zero exit the
 * message implies.
 */
public class ConfigTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // BATS' setup_file. The node lives for the whole class and is stopped automatically.
    classCli.run("solr start").assertSuccess();
  }

  @Test
  public void settingProperty() {
    run("solr create -c config_set_property").assertSuccess();

    // Strengthened: a required-option complaint also means the command refused to run.
    run("solr config -c config_set_property --action set-property"
            + " --property updateHandler.autoCommit.maxDocs"
            + " --solr-url http://localhost:${SOLR_PORT}")
        .assertFailure()
        .assertContains("'value' is a required option.");

    run("solr config -c config_set_property --action set-property"
            + " --property updateHandler.autoCommit.maxDocs --value 100"
            + " --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Successfully set-property updateHandler.autoCommit.maxDocs to 100");

    run("solr config -c config_set_property --action unset-property"
            + " --property updateHandler.autoCommit.maxDocs"
            + " --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Successfully unset-property updateHandler.autoCommit.maxDocs");
  }

  @Test
  public void shortFormOfSettingProperty() {
    run("solr create -c config_short_form").assertSuccess();

    run("solr config -c config_short_form --property updateHandler.autoCommit.maxDocs --value 100")
        .assertContains("Successfully set-property updateHandler.autoCommit.maxDocs to 100")
        .assertContains("assuming solr url is http://localhost:${SOLR_PORT}.");
  }

  @Test
  public void connectingToSolrViaVariousConnectionTypes() {
    run("solr create -c config_connection_types").assertSuccess();

    run("solr config -c config_connection_types --property updateHandler.autoCommit.maxDocs"
            + " --value 100 -s http://localhost:${SOLR_PORT}")
        .assertContains("Successfully set-property updateHandler.autoCommit.maxDocs to 100");

    run("solr config -c config_connection_types --property updateHandler.autoCommit.maxDocs"
            + " --value 100 -z localhost:${ZK_PORT}")
        .assertContains("Successfully set-property updateHandler.autoCommit.maxDocs to 100");

    run("solr config -c config_connection_types --property updateHandler.autoCommit.maxDocs"
            + " --value 100 --zk-host localhost:${ZK_PORT}")
        .assertContains("Successfully set-property updateHandler.autoCommit.maxDocs to 100");

    run("solr config -c config_connection_types --property updateHandler.autoCommit.maxDocs"
            + " --value 100 --solr-connection http://localhost:${SOLR_PORT}/solr")
        .assertContains("Successfully set-property updateHandler.autoCommit.maxDocs to 100");
  }
}

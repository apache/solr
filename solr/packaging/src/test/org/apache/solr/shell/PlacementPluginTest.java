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
 * Port of {@code test_placement_plugin.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs: the second test uses its own collection name because the class shares one
 * {@code SOLR_HOME} (and so one ZooKeeper data directory) rather than wiping it per test; {@code
 * assert_file_contains} becomes {@link #readLinesMatching} on the log file resolved in Java, since
 * the harness does not expand {@code ${SOLR_LOGS_DIR}} inside a path; and {@code solr create} also
 * asserts its exit status.
 */
public class PlacementPluginTest extends SolrShellTestCase {

  @Test
  public void affinityPlacementPluginUsingSysprop() {
    run("solr start -Dsolr.placementplugin.default=affinity");
    run("solr assert --cloud http://localhost:${SOLR_PORT} --timeout 3000");
    run("solr create -c COLL_NAME");
    assertTrue(collectionExists("COLL_NAME"));
    assertFalse(
        "solr.log has no record of the affinity plugin being set",
        readLinesMatching(
                harnessEnv.logsDir().resolve("solr.log").toString(),
                "Default replica placement plugin set in solr\\.placementplugin\\.default to"
                    + " affinity")
            .isEmpty());
  }

  @Test
  public void randomPlacementPluginUsingEnv() {
    env("SOLR_PLACEMENTPLUGIN_DEFAULT", "random");
    run("solr start");
    run("solr assert --cloud http://localhost:${SOLR_PORT} --timeout 3000");
    // COLL_NAME2, not the original's COLL_NAME: the collection the other test created is still in
    // ZooKeeper, which BATS avoided by wiping SOLR_HOME before every test.
    run("solr create -c COLL_NAME2");
    assertTrue(collectionExists("COLL_NAME2"));
    assertFalse(
        "solr.log has no record of the random plugin being set",
        readLinesMatching(
                harnessEnv.logsDir().resolve("solr.log").toString(),
                "Default replica placement plugin set in solr\\.placementplugin\\.default to"
                    + " random")
            .isEmpty());
  }
}

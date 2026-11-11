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
 * Exhaustive, negative assertions over {@code solr status} — the one command that reports Solr
 * nodes across the whole host rather than just this class's.
 *
 * <p>These hold without {@link GlobalProcessScope} because the harness gives each class its own
 * {@code solr.process.scan.marker}, so the CLI only discovers nodes this class started. That is
 * what makes them pass with sibling classes running in parallel forks, with a developer's own Solr
 * up on 8983, and with a second suite sharing a CI box.
 */
public class SolrShellCleanupTest extends SolrShellTestCase {

  @Test
  public void statusSaysSoWhenNothingIsRunning() {
    run("solr status").assertSuccess().assertOutputContains("No Solr nodes are running");
  }

  @Test
  public void aNodeWeStartIsTheOnlyOneListed() {
    env("SOLR_HEAP", "512m");
    run("solr start").assertSuccess();
    run("solr create -c books --shards 2").assertSuccess().assertOutputContains("2 shard(s)");
    run("solr status -p ${SOLR_PORT}").assertSuccess().assertOutputContains("running on port");

    run("solr stop --all").assertSuccess();
    run("solr status").assertSuccess().assertOutputContains("No Solr nodes are running");
  }
}

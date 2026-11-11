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
 * Port of {@code test_version.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on. The
 * original needs no running Solr, so there is no {@code @BeforeClass}; the clean {@code SOLR_HOME}
 * its {@code common_clean_setup} asked for is what the base class already gives every class.
 */
public class VersionTest extends SolrShellTestCase {

  @Test
  public void versionReturnsSolrVersion() {
    run("solr --version").assertContains("Client version:");
  }

  @Test
  public void versionAsDirectToolCallStillRuns() {
    run("solr version").assertContains("Client version:");
  }
}

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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.SolrTestCaseJ4.DEFAULT_TEST_CORENAME;
import static org.apache.solr.SolrTestCaseJ4.params;
import static org.apache.solr.SolrTestCaseJ4.sdoc;
import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests for the split core V2 endpoint via the generated SolrJ client. */
public class SplitCoreAPITest extends SolrTestCase {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule
        .newCollection(DEFAULT_TEST_CORENAME)
        .withConfigSet(ExternalPaths.DEFAULT_CONFIGSET)
        .create();
  }

  @Test
  public void testSplitWithNoTargetCoreOrPathReturnsError() throws Exception {
    var request = new CoresApi.SplitCore(DEFAULT_TEST_CORENAME);

    final var ex =
        expectThrows(
            RemoteSolrException.class, () -> request.process(solrTestRule.getSolrClient()));
    assertEquals(400, ex.code());
    assertTrue(
        "Expected error about missing targetCore or path",
        ex.getMessage().contains("path") || ex.getMessage().contains("targetCore"));
  }

  @Test
  public void testSplitIntoTargetCores() throws Exception {
    final String sourceCore = "split-source";
    final var targets = List.of("split-target-1", "split-target-2");
    for (String coreName : List.of(sourceCore, targets.get(0), targets.get(1))) {
      solrTestRule.newCollection(coreName).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
    }

    try (var client = solrTestRule.getSolrClient(sourceCore)) {
      client.add(List.of(sdoc("id", "1"), sdoc("id", "2"), sdoc("id", "3")));
      client.commit();
    }

    final var request = new CoresApi.SplitCore(sourceCore);
    request.setTargetCore(targets);
    final var response = request.process(solrTestRule.getAdminClient());
    assertEquals(0, response.responseHeader.status);

    long totalDocuments = 0;
    for (String target : targets) {
      try (var client = solrTestRule.getSolrClient(target)) {
        client.commit();
        totalDocuments += client.query(params("q", "*:*", "rows", "0")).getResults().getNumFound();
      }
    }
    assertEquals(3, totalDocuments);
  }
}

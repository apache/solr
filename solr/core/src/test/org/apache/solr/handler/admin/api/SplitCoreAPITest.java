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
import static org.hamcrest.Matchers.containsString;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.GetNodeCommandStatusResponse;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests for the split core V2 endpoint via the generated SolrJ client. */
public class SplitCoreAPITest extends SolrTestCase {

  private static final Duration ASYNC_SPLIT_TIMEOUT = Duration.ofSeconds(30);

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
    final var targets = createSplitCores(sourceCore);

    final var request = new CoresApi.SplitCore(sourceCore);
    request.setTargetCore(targets);
    final var response = request.process(solrTestRule.getAdminClient());
    assertEquals(0, response.responseHeader.status);
    assertNull(response.requestId);
    assertNotNull(response.timing);
    assertTrue(response.timing.get("time") instanceof Number);
    assertTrue(response.timing.get("doSplit") instanceof Map<?, ?>);
    assertSplitDocuments(targets);
  }

  private void assertSplitDocuments(List<String> targets) throws Exception {
    long totalDocuments = 0;
    for (String target : targets) {
      try (var client = solrTestRule.getSolrClient(target)) {
        client.commit();
        totalDocuments += client.query(params("q", "*:*", "rows", "0")).getResults().getNumFound();
      }
    }
    assertEquals(3, totalDocuments);
  }

  private List<String> createSplitCores(String sourceCore) throws Exception {
    final var targets = List.of(sourceCore + "-target-1", sourceCore + "-target-2");
    for (String coreName : List.of(sourceCore, targets.get(0), targets.get(1))) {
      solrTestRule.newCollection(coreName).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
    }
    try (var client = solrTestRule.getSolrClient(sourceCore)) {
      client.add(List.of(sdoc("id", "1"), sdoc("id", "2"), sdoc("id", "3")));
      client.commit();
    }
    return targets;
  }

  @Test
  public void testUnknownCoreReturnsNotFound() {
    for (boolean getRanges : List.of(false, true)) {
      final var request = new CoresApi.SplitCore("missing-source");
      request.setTargetCore(List.of(DEFAULT_TEST_CORENAME));
      request.setGetRanges(getRanges);
      final var error =
          expectThrows(
              RemoteSolrException.class, () -> request.process(solrTestRule.getAdminClient()));
      assertEquals(404, error.code());
      assertThat(error.getMessage(), containsString("Unknown core missing-source"));
    }
  }

  @Test
  public void testV1UnknownCoreKeepsLegacyStatus() {
    for (boolean getRanges : List.of(false, true)) {
      final var parameters =
          params(
              "action",
              "SPLIT",
              "core",
              "missing-source",
              "targetCore",
              DEFAULT_TEST_CORENAME,
              "getRanges",
              Boolean.toString(getRanges));
      final var request =
          new GenericSolrRequest(
              SolrRequest.METHOD.GET,
              "/admin/cores",
              SolrRequest.SolrRequestType.ADMIN,
              parameters);
      final var error =
          expectThrows(
              RemoteSolrException.class, () -> request.process(solrTestRule.getAdminClient()));
      assertEquals(getRanges ? 400 : 500, error.code());
    }
  }

  @Test
  public void testV1SplitResponseKeepsLegacyTiming() throws Exception {
    final String sourceCore = "legacy-split-source";
    final var targets = createSplitCores(sourceCore);
    final var parameters = params("action", "SPLIT", "core", sourceCore);
    parameters.add("targetCore", targets.toArray(String[]::new));
    final var request =
        new GenericSolrRequest(
            SolrRequest.METHOD.GET, "/admin/cores", SolrRequest.SolrRequestType.ADMIN, parameters);
    final var response = request.process(solrTestRule.getAdminClient()).getResponse();
    assertNull(response.get("timing"));
    assertNull(response.get("requestid"));
    assertNotNull(((NamedList<?>) response.get("response")).get("timing"));
    assertSplitDocuments(targets);
  }

  @Test
  public void testAsyncSplitReturnsRequestId() throws Exception {
    final String sourceCore = "async-split-source";
    final var targets = createSplitCores(sourceCore);
    final String requestId = "split-request-" + random().nextLong();
    final var request = new CoresApi.SplitCore(sourceCore);
    request.setTargetCore(targets);
    request.setAsync(requestId);
    final var response = request.process(solrTestRule.getAdminClient());
    assertEquals(requestId, response.requestId);

    final var status = new AtomicReference<GetNodeCommandStatusResponse>();
    new TimeOut(ASYNC_SPLIT_TIMEOUT, TimeSource.NANO_TIME)
        .waitFor(
            "Split task did not complete",
            () -> {
              try {
                status.set(
                    new NodeApi.GetCommandStatus(requestId).process(solrTestRule.getAdminClient()));
                assertNotEquals("failed", status.get().status);
                return "completed".equals(status.get().status);
              } catch (Exception e) {
                throw new AssertionError(e);
              }
            });
    assertNotNull(((Map<?, ?>) status.get().response).get("timing"));
    assertSplitDocuments(targets);
  }

  @Test
  public void testGetRangesWithAsyncDoesNotEnqueueTask() throws Exception {
    final String requestId = "split-ranges-" + random().nextLong();
    final var request = new CoresApi.SplitCore(DEFAULT_TEST_CORENAME);
    request.setGetRanges(true);
    request.setAsync(requestId);
    final var error =
        expectThrows(
            RemoteSolrException.class, () -> request.process(solrTestRule.getAdminClient()));
    assertEquals(400, error.code());
    assertThat(error.getMessage(), containsString("requires SolrCloud mode"));
    assertEquals(
        "notfound",
        new NodeApi.GetCommandStatus(requestId).process(solrTestRule.getAdminClient()).status);
  }
}

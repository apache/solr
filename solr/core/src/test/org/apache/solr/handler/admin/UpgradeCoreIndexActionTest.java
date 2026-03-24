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
package org.apache.solr.handler.admin;

import static org.hamcrest.CoreMatchers.containsString;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.Version;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.RefCounted;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpgradeCoreIndexActionTest extends SolrTestCaseJ4 {
  private static final int DOCS_PER_SEGMENT = 3;
  private static final String DV_FIELD = "dvonly_i_dvo";

  private static VarHandle segmentInfoMinVersionHandle;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-nomergepolicyfactory.xml", "schema.xml");
    segmentInfoMinVersionHandle =
        MethodHandles.privateLookupIn(SegmentInfo.class, MethodHandles.lookup())
            .findVarHandle(SegmentInfo.class, "minVersion", Version.class);
  }

  @Before
  public void resetIndex() {
    assertU(delQ("*:*"));
    assertU(commit("openSearcher", "true"));
  }

  @Test
  public void testUpgradeCoreIndexSelectiveReindexDeletesOldSegments() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();

    final SegmentLayout layout = buildThreeSegments(coreName);
    final Version simulatedOldMinVersion = Version.fromBits(Version.LATEST.major - 1, 0, 0);

    // Simulate:
    // - seg1: "pure 9x" (minVersion=9)
    // - seg2: "pure 10x" (minVersion=10)
    // - seg3: "minVersion 9x, version 10x" (merged segment; minVersion=9)
    setMinVersionForSegments(core, Set.of(layout.seg1, layout.seg3), simulatedOldMinVersion);

    final Set<String> segmentsBeforeUpgrade = listSegmentNames(core);

    CoreAdminHandler admin = new CoreAdminHandler(h.getCoreContainer());
    try {
      final SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.UPGRADECOREINDEX.toString(),
              CoreAdminParams.CORE,
              coreName),
          resp);

      assertNull("Unexpected exception: " + resp.getException(), resp.getException());
      assertEquals(coreName, resp.getValues().get("core"));
      assertEquals(2, resp.getValues().get("numSegmentsEligibleForUpgrade"));
      assertEquals(2, resp.getValues().get("numSegmentsUpgraded"));
      assertEquals("UPGRADE_SUCCESSFUL", resp.getValues().get("upgradeStatus"));
    } finally {
      admin.shutdown();
      admin.close();
    }

    // The action commits internally and reopens the searcher; verify segments on disk.
    final Set<String> segmentsAfter = listSegmentNames(core);
    final Set<String> newSegments = new HashSet<>(segmentsAfter);
    newSegments.removeAll(segmentsBeforeUpgrade);
    assertFalse(
        "Expected at least one new segment to be created by reindexing", newSegments.isEmpty());
    assertTrue("Expected seg2 to remain", segmentsAfter.contains(layout.seg2));
    assertFalse("Expected seg1 to be dropped", segmentsAfter.contains(layout.seg1));
    assertFalse("Expected seg3 to be dropped", segmentsAfter.contains(layout.seg3));

    // Searcher was reopened by the action's commit; verify document count and field values.
    assertQ(req("q", "*:*"), "//result[@numFound='" + (3 * DOCS_PER_SEGMENT) + "']");

    // Validate docValues-only (non-stored) fields were preserved for reindexed documents.
    // seg1 and seg3 were reindexed; seg2 was not.
    assertDocValuesOnlyFieldPreserved();
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testUpgradeCoreIndexAsyncRequestStatusContainsOperationResponse() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();

    final SegmentLayout layout = buildThreeSegments(coreName);
    final Version simulatedOldMinVersion = Version.fromBits(Version.LATEST.major - 1, 0, 0);
    setMinVersionForSegments(core, Set.of(layout.seg1, layout.seg3), simulatedOldMinVersion);

    final Set<String> segmentsBeforeUpgrade = listSegmentNames(core);

    final String requestId = "upgradecoreindex_async_1";
    CoreAdminHandler admin = new CoreAdminHandler(h.getCoreContainer());
    try {
      SolrQueryResponse submitResp = new SolrQueryResponse();
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.UPGRADECOREINDEX.toString(),
              CoreAdminParams.CORE,
              coreName,
              CommonAdminParams.ASYNC,
              requestId),
          submitResp);
      assertNull(submitResp.getException());

      SolrQueryResponse statusResp = new SolrQueryResponse();
      int maxRetries = 60;
      while (maxRetries-- > 0) {
        statusResp = new SolrQueryResponse();
        admin.handleRequestBody(
            req(
                CoreAdminParams.ACTION,
                CoreAdminParams.CoreAdminAction.REQUESTSTATUS.toString(),
                CoreAdminParams.REQUESTID,
                requestId),
            statusResp);

        if ("completed".equals(statusResp.getValues().get("STATUS"))) {
          break;
        }
        Thread.sleep(250);
      }

      assertEquals("completed", statusResp.getValues().get("STATUS"));
      Object opResponse = statusResp.getValues().get("response");
      assertNotNull(opResponse);
      assertTrue("Expected map response, got: " + opResponse.getClass(), opResponse instanceof Map);

      Map<String, Object> opResponseMap = (Map<String, Object>) opResponse;
      assertEquals(coreName, opResponseMap.get("core"));
      assertEquals(2, ((Number) opResponseMap.get("numSegmentsEligibleForUpgrade")).intValue());
      assertEquals(2, ((Number) opResponseMap.get("numSegmentsUpgraded")).intValue());
      assertEquals("UPGRADE_SUCCESSFUL", opResponseMap.get("upgradeStatus"));
    } finally {
      admin.shutdown();
      admin.close();
    }

    final Set<String> segmentsAfter = listSegmentNames(core);
    final Set<String> newSegments = new HashSet<>(segmentsAfter);
    newSegments.removeAll(segmentsBeforeUpgrade);
    assertFalse(
        "Expected at least one new segment to be created by reindexing", newSegments.isEmpty());
    assertTrue("Expected seg2 to remain", segmentsAfter.contains(layout.seg2));
    assertFalse("Expected seg1 to be dropped", segmentsAfter.contains(layout.seg1));
    assertFalse("Expected seg3 to be dropped", segmentsAfter.contains(layout.seg3));

    // Validate docValues-only (non-stored) fields were preserved for reindexed documents.
    assertDocValuesOnlyFieldPreserved();
  }

  @Test
  public void testNoUpgradeNeededWhenAllSegmentsCurrent() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();

    // Index documents and commit - all segments will be at the current Lucene version
    for (int i = 0; i < DOCS_PER_SEGMENT; i++) {
      assertU(adoc("id", Integer.toString(i)));
    }
    assertU(commit("openSearcher", "true"));

    final Set<String> segmentsBefore = listSegmentNames(core);
    assertFalse("Expected at least one segment", segmentsBefore.isEmpty());

    CoreAdminHandler admin = new CoreAdminHandler(h.getCoreContainer());
    try {
      final SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.UPGRADECOREINDEX.toString(),
              CoreAdminParams.CORE,
              coreName),
          resp);

      assertNull("Unexpected exception: " + resp.getException(), resp.getException());
      assertEquals(coreName, resp.getValues().get("core"));
      assertEquals(0, resp.getValues().get("numSegmentsEligibleForUpgrade"));
      assertEquals("NO_UPGRADE_NEEDED", resp.getValues().get("upgradeStatus"));
    } finally {
      admin.shutdown();
      admin.close();
    }

    // Verify no segments were modified
    final Set<String> segmentsAfter = listSegmentNames(core);
    assertEquals("Segments should remain unchanged", segmentsBefore, segmentsAfter);

    // Verify documents are still queryable
    assertQ(req("q", "*:*"), "//result[@numFound='" + DOCS_PER_SEGMENT + "']");
  }

  private SegmentLayout buildThreeSegments(String coreName) throws Exception {
    final SolrCore core = h.getCore();

    Set<String> segmentsBefore = listSegmentNames(core);
    indexDocs(0);
    final String seg1 = commitAndGetNewSegment(core, segmentsBefore);
    segmentsBefore = listSegmentNames(core);

    indexDocs(1000);
    final String seg2 = commitAndGetNewSegment(core, segmentsBefore);
    segmentsBefore = listSegmentNames(core);

    indexDocs(2000);
    final String seg3 = commitAndGetNewSegment(core, segmentsBefore);

    Set<String> allSegments = listSegmentNames(core);
    assertTrue(allSegments.contains(seg1));
    assertTrue(allSegments.contains(seg2));
    assertTrue(allSegments.contains(seg3));

    return new SegmentLayout(coreName, seg1, seg2, seg3);
  }

  private void indexDocs(int baseId) {
    for (int i = 0; i < DOCS_PER_SEGMENT; i++) {
      // schema.xml copies id into numeric fields; use numeric IDs to avoid parsing errors
      final String id = Integer.toString(baseId + i);
      assertU(adoc("id", id, DV_FIELD, Integer.toString(baseId + i + 10_000), "title", "t" + id));
    }
  }

  private void assertDocValuesOnlyFieldPreserved() {
    // Assert one doc that must have been reindexed (seg1) and one from seg3.
    assertDocHasDvFieldValue(0, 10_000);
    assertDocHasDvFieldValue(2000, 12_000);

    // Also sanity-check a doc from the untouched segment (seg2) still has its value.
    assertDocHasDvFieldValue(1000, 11_000);
  }

  private void assertDocHasDvFieldValue(int id, int expected) {
    assertQ(
        req("q", "id:" + id, "fl", "id," + DV_FIELD),
        "//result[@numFound='1']",
        "//result/doc/int[@name='" + DV_FIELD + "'][.='" + expected + "']");
  }

  private String commitAndGetNewSegment(SolrCore core, Set<String> segmentsBefore)
      throws Exception {
    assertU(commit("openSearcher", "true"));
    Set<String> segmentsAfter = new HashSet<>(listSegmentNames(core));
    segmentsAfter.removeAll(new HashSet<>(segmentsBefore));
    assertEquals("Expected exactly one new segment", 1, segmentsAfter.size());
    return segmentsAfter.iterator().next();
  }

  private Set<String> listSegmentNames(SolrCore core) throws Exception {
    return core.withSearcher(
        searcher -> {
          final Set<String> segmentNames = new HashSet<>();
          for (LeafReaderContext ctx : searcher.getTopReaderContext().leaves()) {
            SegmentReader segmentReader = (SegmentReader) FilterLeafReader.unwrap(ctx.reader());
            segmentNames.add(segmentReader.getSegmentName());
          }
          return segmentNames;
        });
  }

  private void setMinVersionForSegments(SolrCore core, Set<String> segments, Version minVersion) {
    RefCounted<SolrIndexSearcher> searcherRef = core.getSearcher();
    try {
      final List<LeafReaderContext> leaves = searcherRef.get().getTopReaderContext().leaves();
      for (LeafReaderContext ctx : leaves) {
        SegmentReader segmentReader = (SegmentReader) FilterLeafReader.unwrap(ctx.reader());
        if (!segments.contains(segmentReader.getSegmentName())) {
          continue;
        }
        final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;
        segmentInfoMinVersionHandle.set(segmentInfo, minVersion);
      }
    } finally {
      searcherRef.decref();
    }
  }

  private record SegmentLayout(String coreName, String seg1, String seg2, String seg3) {}

  @Test
  public void testUpgradeCoreIndexFailsWithNestedDocuments() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();

    // Create a parent document with a child document (nested doc)
    SolrInputDocument parentDoc = new SolrInputDocument();
    parentDoc.addField("id", "100");
    parentDoc.addField("title", "Parent Document");

    SolrInputDocument childDoc = new SolrInputDocument();
    childDoc.addField("id", "101");
    childDoc.addField("title", "Child Document");

    parentDoc.addChildDocument(childDoc);

    // Index the nested document
    try (SolrQueryRequestBase req = new SolrQueryRequestBase(core, new ModifiableSolrParams())) {
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.solrDoc = parentDoc;
      core.getUpdateHandler().addDoc(cmd);
    }
    assertU(commit("openSearcher", "true"));

    // Verify documents were indexed (parent + child = 2 docs)
    assertQ(req("q", "*:*"), "//result[@numFound='2']");

    // Attempt to upgrade the index - should fail because of nested documents
    CoreAdminHandler admin = new CoreAdminHandler(h.getCoreContainer());
    try {
      final SolrQueryResponse resp = new SolrQueryResponse();
      SolrException thrown =
          assertThrows(
              SolrException.class,
              () ->
                  admin.handleRequestBody(
                      req(
                          CoreAdminParams.ACTION,
                          CoreAdminParams.CoreAdminAction.UPGRADECOREINDEX.toString(),
                          CoreAdminParams.CORE,
                          coreName),
                      resp));

      // Verify the exception message indicates nested documents are not supported
      assertThat(
          thrown.getMessage(),
          containsString("does not support indexes containing nested documents"));
    } finally {
      admin.shutdown();
      admin.close();
    }
  }
}

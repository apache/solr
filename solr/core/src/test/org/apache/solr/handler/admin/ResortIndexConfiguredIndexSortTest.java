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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies the SOLR-13681/SOLR-12239 migration workflow: with the index sort configured directly
 * via {@code <indexSort>}, {@code RESORTINDEX} (no {@code sort} param) picks up that configured
 * sort and physically re-sorts the existing (unsorted) segments.
 */
public class ResortIndexConfiguredIndexSortTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // This config declares <indexSort>timestamp_i_dvo desc</indexSort> and no SortingMergePolicy.
    initCore("solrconfig-indexsort.xml", "schema.xml");
  }

  @Test
  public void testResortUsesConfiguredIndexSortWhenNoSortParam() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();

    // Index documents in an order that does NOT match the configured desc sort. id == timestamp so
    // the stored id doubles as the (docValues-only) timestamp for verification below.
    int[] vals = {2, 0, 4, 1, 3};
    for (int v : vals) {
      assertU(adoc("id", Integer.toString(v), "timestamp_i_dvo", Integer.toString(v)));
    }
    assertU(commit());

    CoreAdminHandler admin = new CoreAdminHandler(h.getCoreContainer());
    try {
      final SolrQueryResponse resp = new SolrQueryResponse();
      // No 'sort' param -> must fall back to the configured <indexSort>, not error out.
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.RESORTINDEX.toString(),
              CoreAdminParams.CORE,
              coreName),
          resp);
      assertNull("Unexpected exception: " + resp.getException(), resp.getException());
      String applied = String.valueOf(resp.getValues().get("indexSort"));
      assertTrue(
          "expected the configured <indexSort> (on timestamp_i_dvo) in response, got: " + applied,
          applied.contains("timestamp_i_dvo"));
    } finally {
      admin.shutdown();
      admin.close();
    }

    assertQ(req("q", "*:*", "rows", "0"), "//result[@numFound='5']");

    // The index is now physically sorted by timestamp_i_dvo descending (internal docid order),
    // which is what lets a matching query terminate early per segment.
    RefCounted<SolrIndexSearcher> ref = core.getSearcher();
    try {
      var storedFields = ref.get().getIndexReader().storedFields();
      int maxDoc = ref.get().getIndexReader().maxDoc();
      long prev = Long.MAX_VALUE;
      for (int i = 0; i < maxDoc; i++) {
        // id == timestamp_i_dvo in this data; id is stored, the timestamp is docValues-only.
        long ts = Long.parseLong(storedFields.document(i).get("id"));
        assertTrue(
            "internal docids must be timestamp-descending after resort: " + prev + " then " + ts,
            ts <= prev);
        prev = ts;
      }
    } finally {
      ref.decref();
    }
  }
}

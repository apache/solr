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
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that {@code RESORTINDEX} falls back to the sort configured on the core (via a
 * SortingMergePolicy) when no explicit {@code sort} parameter is given (SOLR-12239).
 */
public class ResortIndexConfiguredSortTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // This config declares a SortingMergePolicy with sort = "timestamp_i_dvo desc".
    initCore("solrconfig-sortingmergepolicyfactory.xml", "schema.xml");
  }

  @Test
  public void testResortUsesConfiguredSortWhenNoSortParam() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();
    for (int i = 0; i < 5; i++) {
      assertU(adoc("id", Integer.toString(i), "timestamp_i_dvo", Integer.toString(i)));
    }
    assertU(commit());

    CoreAdminHandler admin = new CoreAdminHandler(h.getCoreContainer());
    try {
      final SolrQueryResponse resp = new SolrQueryResponse();
      // No 'sort' param -> must fall back to the configured SortingMergePolicy sort.
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.RESORTINDEX.toString(),
              CoreAdminParams.CORE,
              coreName),
          resp);
      assertNull("Unexpected exception: " + resp.getException(), resp.getException());
      assertEquals(coreName, resp.getValues().get("core"));
      // The applied sort should be the one configured on the core (on timestamp_i_dvo), proving the
      // fallback used the configured SortingMergePolicy sort rather than requiring a param.
      String applied = String.valueOf(resp.getValues().get("indexSort"));
      assertTrue(
          "expected configured sort (on timestamp_i_dvo) in response, got: " + applied,
          applied.contains("timestamp_i_dvo"));
    } finally {
      admin.shutdown();
      admin.close();
    }

    assertQ(req("q", "*:*", "rows", "0"), "//result[@numFound='5']");
  }
}

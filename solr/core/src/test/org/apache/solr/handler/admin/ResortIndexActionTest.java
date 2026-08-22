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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
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

/**
 * End-to-end test of the {@code RESORTINDEX} core-admin action (SOLR-12239): re-sort an existing
 * (unsorted) core index and swap it in, so the core serves a sorted index without reindexing.
 */
public class ResortIndexActionTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Plain config, no index sort configured -> the initial index is UNSORTED.
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void clearIndexBefore() {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testResortIndexAction() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();

    // Build an existing unsorted index (intDvoDefault values out of order).
    int[] vals = {5, 1, 4, 2, 3, 0};
    for (int v : vals) {
      assertU(adoc("id", Integer.toString(v), "intDvoDefault", Integer.toString(v)));
    }
    assertU(commit());
    assertQ(req("q", "*:*", "rows", "0"), "//result[@numFound='6']");

    CoreAdminHandler admin = new CoreAdminHandler(h.getCoreContainer());
    try {
      final SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.RESORTINDEX.toString(),
              CoreAdminParams.CORE,
              coreName,
              CommonParams.SORT,
              "intDvoDefault asc"),
          resp);
      assertNull("Unexpected exception: " + resp.getException(), resp.getException());
      assertEquals(coreName, resp.getValues().get("core"));
    } finally {
      admin.shutdown();
      admin.close();
    }

    // All docs still present and queryable after the resort+swap.
    assertQ(req("q", "*:*", "rows", "0"), "//result[@numFound='6']");

    // The index is now physically sorted by intDvoDefault ascending (internal docid order).
    RefCounted<SolrIndexSearcher> ref = core.getSearcher();
    try {
      var storedFields = ref.get().getIndexReader().storedFields();
      int maxDoc = ref.get().getIndexReader().maxDoc();
      long prev = Long.MIN_VALUE;
      for (int i = 0; i < maxDoc; i++) {
        long id = Long.parseLong(storedFields.document(i).get("id"));
        assertTrue(
            "internal docids must be ascending after resort: " + prev + " then " + id, id >= prev);
        prev = id;
      }
      assertEquals(
          "smallest value sorts first", 0L, Long.parseLong(storedFields.document(0).get("id")));
    } finally {
      ref.decref();
    }
  }

  @Test
  public void testResortIndexRequiresSortWhenNoneConfigured() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();
    assertU(adoc("id", "1", "intDvoDefault", "1"));
    assertU(commit());

    // No 'sort' param, and this core has no SortingMergePolicy configured -> clear error.
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
                          CoreAdminParams.CoreAdminAction.RESORTINDEX.toString(),
                          CoreAdminParams.CORE,
                          coreName),
                      resp));
      assertThat(thrown.getMessage(), containsString("no configured index sort"));
    } finally {
      admin.shutdown();
      admin.close();
    }
  }

  @Test
  public void testResortIndexRejectsUnparseableSort() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();
    assertU(adoc("id", "1", "intDvoDefault", "1"));
    assertU(commit());

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
                          CoreAdminParams.CoreAdminAction.RESORTINDEX.toString(),
                          CoreAdminParams.CORE,
                          coreName,
                          CommonParams.SORT,
                          "no_such_field asc"),
                      resp));
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, thrown.code());
    } finally {
      admin.shutdown();
      admin.close();
    }
  }

  @Test
  public void testResortIndexRejectsChildDocs() throws Exception {
    final SolrCore core = h.getCore();
    final String coreName = core.getName();

    // Index a parent+child block; re-sorting would break the block, so it must be rejected.
    SolrInputDocument parent = new SolrInputDocument();
    parent.addField("id", "100");
    parent.addField("title", "parent");
    SolrInputDocument child = new SolrInputDocument();
    child.addField("id", "101");
    child.addField("title", "child");
    parent.addChildDocument(child);
    try (SolrQueryRequestBase addReq =
        new SolrQueryRequestBase(core, new ModifiableSolrParams()) {}) {
      AddUpdateCommand cmd = new AddUpdateCommand(addReq);
      cmd.solrDoc = parent;
      core.getUpdateHandler().addDoc(cmd);
    }
    assertU(commit());

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
                          CoreAdminParams.CoreAdminAction.RESORTINDEX.toString(),
                          CoreAdminParams.CORE,
                          coreName,
                          CommonParams.SORT,
                          "intDvoDefault asc"),
                      resp));
      assertThat(thrown.getMessage(), containsString("child/nested documents"));
    } finally {
      admin.shutdown();
      admin.close();
    }
  }
}

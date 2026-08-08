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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.ResortCoreIndexRequestBody;
import org.apache.solr.client.api.model.ResortCoreIndexResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests the {@link ResortCoreIndex} V2 API directly (SOLR-12239). */
public class ResortCoreIndexTest extends SolrTestCaseJ4 {

  private CoreContainer coreContainer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void setup() {
    coreContainer = h.getCoreContainer();
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @After
  public void cleanup() {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testResortViaV2Api() throws Exception {
    final String coreName = h.getCore().getName();
    int[] vals = {5, 1, 4, 2, 3, 0};
    for (int v : vals) {
      assertU(adoc("id", Integer.toString(v), "intDvoDefault", Integer.toString(v)));
    }
    assertU(commit());

    final SolrQueryRequest req = req();
    try {
      ResortCoreIndex api =
          new ResortCoreIndex(
              coreContainer,
              new CoreAdminHandler.CoreAdminAsyncTracker(),
              req,
              new SolrQueryResponse());
      ResortCoreIndexRequestBody body = new ResortCoreIndexRequestBody();
      body.sort = "intDvoDefault asc";

      ResortCoreIndexResponse response = api.resortCoreIndex(coreName, body);
      assertEquals(coreName, response.core);
      assertNotNull(response.indexSort);
    } finally {
      req.close();
    }

    // Index is now physically sorted ascending by intDvoDefault.
    assertQ(req("q", "*:*", "rows", "0"), "//result[@numFound='6']");
    var ref = h.getCore().getSearcher();
    try {
      var storedFields = ref.get().getIndexReader().storedFields();
      long prev = Long.MIN_VALUE;
      for (int i = 0; i < ref.get().getIndexReader().maxDoc(); i++) {
        long id = Long.parseLong(storedFields.document(i).get("id"));
        assertTrue("ascending after resort: " + prev + " then " + id, id >= prev);
        prev = id;
      }
      assertEquals("smallest first", 0L, Long.parseLong(storedFields.document(0).get("id")));
    } finally {
      ref.decref();
    }
  }
}

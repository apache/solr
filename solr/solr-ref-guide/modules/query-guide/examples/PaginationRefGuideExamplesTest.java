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
package org.apache.solr.client.ref_guide_examples;

import static org.apache.solr.client.ref_guide_examples.ExpectedOutputVerifier.clear;
import static org.apache.solr.client.ref_guide_examples.ExpectedOutputVerifier.ensureNoLeftoverOutputExpectations;
import static org.apache.solr.client.ref_guide_examples.ExpectedOutputVerifier.expectLine;
import static org.apache.solr.client.ref_guide_examples.ExpectedOutputVerifier.print;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example Cursormark usage in SolrJ.
 *
 * <p>Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference
 * Guide.
 */
public class PaginationRefGuideExamplesTest extends SolrCloudTestCase {
  private static final int NUM_INDEXED_DOCUMENTS = 9;
  private static final int BATCH_SIZE = 1;
  private static final String COLLECTION_NAME = "techproducts";
  private static final String CONFIG_NAME = "techproducts_config";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig(CONFIG_NAME, ExternalPaths.TECHPRODUCTS_CONFIGSET).configure();

    CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1)
        .process(cluster.getSolrClient());
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    clear();
    indexSampleData();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    ensureNoLeftoverOutputExpectations();
  }

  @Test
  public void testCursormarkWithSolrJExample() throws Exception {
    for (int i = 0; i < NUM_INDEXED_DOCUMENTS; i++) {
      expectLine("ID: " + i + "; Name: Fitbit Model " + i);
    }

    // spotless:off
    // tag::cursormark-query[]
    SolrQuery q =
        new SolrQuery("*:*")
            .setRows(BATCH_SIZE)
            .setSort(SolrQuery.SortClause.asc("id"));
    String cursorMark = CursorMarkParams.CURSOR_MARK_START;
    boolean done = false;

    while (!done) {
      q.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      QueryResponse rsp = cluster.getSolrClient().query(COLLECTION_NAME, q);
      String nextCursorMark = rsp.getNextCursorMark();
      for (SolrDocument doc : rsp.getResults()) {
        final String docOutput =
            String.format(
                Locale.ROOT,
                "ID: %s; Name: %s",
                doc.getFieldValue("id"),
                doc.getFieldValue("name"));
        print(docOutput);
      }

      done = cursorMark.equals(nextCursorMark);
      cursorMark = nextCursorMark;
    }
    // end::cursormark-query[]
    // spotless:on
  }

  private void indexSampleData() throws Exception {
    final SolrClient client = cluster.getSolrClient();

    final List<SolrInputDocument> docList = new ArrayList<>();
    for (int i = 0; i < NUM_INDEXED_DOCUMENTS; i++) {
      final SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", "Fitbit Model " + i);
      docList.add(doc);
    }

    client.add(COLLECTION_NAME, docList);
    client.commit(COLLECTION_NAME);
  }
}

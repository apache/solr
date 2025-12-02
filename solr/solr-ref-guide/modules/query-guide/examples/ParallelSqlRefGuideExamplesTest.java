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

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example SolrJ usage of the Parallel SQL interface.
 *
 * <p>Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference
 * Guide.
 */
public class ParallelSqlRefGuideExamplesTest extends SolrCloudTestCase {
  private static final int NUM_INDEXED_DOCUMENTS = 3;
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
  public void testQueryOnJdbcSqlInterface() throws Exception {
    // expectLine("Item: 0; Price: 0.0");
    // expectLine("Item: 1; Price: 2.0");
    // expectLine("Item: 2; Price: 4.0");

    final String zkHost = cluster.getZkServer().getZkAddress();
    // tag::jdbc-query-interface[]
    final String connString =
        "jdbc:solr://"
            + zkHost
            + "?collection=techproducts&aggregationMode=map_reduce&numWorkers=2";

    // try (Connection con = DriverManager.getConnection(connString)) {
    // GETTING A ObjectTracker error when I call this.
    // try (final Statement stmt = con.createStatement()) {
    // final String sqlQuery = "SELECT id, price_f FROM techproducts LIMIT 3";

    //        try (ResultSet rs = stmt.executeQuery(sqlQuery)) {
    //          while (rs.next()) {
    //            final String resultString =
    //                String.format("Item: %s; Price: %s", rs.getString("id"),
    // rs.getString("price_f"));
    //            print(resultString);
    //          }
    //        }
    // }
    // }
    // end::jdbc-query-interface[]
  }

  private void indexSampleData() throws Exception {
    final SolrClient client = cluster.getSolrClient();

    final List<SolrInputDocument> docList = new ArrayList<>();
    for (int i = 0; i < NUM_INDEXED_DOCUMENTS; i++) {
      final SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("price_f", i * 2.0f);
      docList.add(doc);
    }

    client.add(COLLECTION_NAME, docList);
    client.commit(COLLECTION_NAME);
  }
}

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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.TermsResponse.Term;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example SolrJ usage of the Terms Component.
 *
 * <p>Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference
 * Guide.
 */
public class TermComponentRefGuideExamplesTest extends SolrCloudTestCase {
  private static final String COLLECTION_NAME = "techproducts";
  private static final String CONFIG_NAME = "techproducts_config";

  public static class TechProduct {
    @Field public String id;
    @Field public String name;

    public TechProduct() {}

    public TechProduct(String id, String name) {
      this.id = id;
      this.name = name;
    }
  }

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

  @Test
  public void testTermsComponentRequest() throws Exception {
    expectLine("Top term is fitbit with frequency 2");

    // tag::term-component-request[]
    final SolrQuery termsQuery = new SolrQuery();
    termsQuery.setRequestHandler("/terms");
    termsQuery.setTerms(true);
    termsQuery.setTermsLimit(5);
    termsQuery.addTermsField("name");
    termsQuery.setTermsMinCount(1);

    final QueryRequest request = new QueryRequest(termsQuery);
    final List<Term> terms =
        request
            .process(cluster.getSolrClient(), COLLECTION_NAME)
            .getTermsResponse()
            .getTerms("name");

    final Term topTerm = terms.get(0);
    print("Top term is " + topTerm.getTerm() + " with frequency " + topTerm.getFrequency());
    // end::term-component-request[]
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    ensureNoLeftoverOutputExpectations();
  }

  private void indexSampleData() throws Exception {
    SolrClient client = cluster.getSolrClient();

    final List<TechProduct> products = new ArrayList<>();
    products.add(new TechProduct("1", "Fitbit Alta"));
    products.add(new TechProduct("2", "Sony Walkman"));
    products.add(new TechProduct("3", "Garmin GPS"));
    products.add(new TechProduct("4", "Fitbit Flex"));

    client.addBeans(COLLECTION_NAME, products);
    client.commit(COLLECTION_NAME);
  }
}

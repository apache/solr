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
package org.apache.solr.cloud;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;

public class IllegalStateExceptionManagedSynonymGraphFilterTest extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";
  private static final String CONFIG_NAME = "myconf";

  private CloseableHttpClient httpClient;
  private CloudSolrClient solrClient;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(CONFIG_NAME, configset("cloud-managed-resource"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    solrClient = getCloudSolrClient(cluster);
    httpClient = (CloseableHttpClient) solrClient.getHttpClient();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    IOUtils.close(solrClient, httpClient);

    cluster.deleteAllCollections();
  }

  @Test
  public void test() throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION, CONFIG_NAME, 2, 1).process(solrClient);

    cluster.waitForActiveCollection(COLLECTION, 2, 2);

    waitForState("Expected collection1 to be created with 2 shards and 1 replica", COLLECTION, clusterShape(2, 2));

    // index time exception
    try {
      new UpdateRequest()
          .add("id", "6", "text_syn", "humpty dumpy sat on a wall")
          .add("id", "7", "text_syn", "humpty dumpy3 sat on a walls")
          .add("id", "8", "text_syn", "humpty dumpy2 sat on a walled")
          .commit(solrClient, COLLECTION);
      MatcherAssert.assertThat("Exception should be raised - so should not raise AssertionError", 0, is(1));
    } catch (SolrException e) {
      MatcherAssert.assertThat(e.getRootThrowable(), is("java.lang.IllegalStateException"));
      MatcherAssert.assertThat(e.toString(), containsString("Exception writing document id"));
      MatcherAssert.assertThat(e.toString(), containsString("to the index; possible analysis error."));
    }

    // query time exception
    final SolrQuery solrQuery = new SolrQuery("q", "text_syn:dumpy2", "rows", "0");
    try {
      solrClient.query(COLLECTION, solrQuery);
      MatcherAssert.assertThat("Exception should be raised - so should not raise AssertionError", 0, is(1));
    } catch (Exception e) {
      MatcherAssert.assertThat((((SolrServerException) e).getRootCause()).getMessage(), containsString("org.apache.solr.rest.schema.analysis.ManagedSynonymGraphFilterFactory not initialized correctly! The SynonymFilterFactory delegate was not initialized."));
    }

    // no exception after reloading
    CollectionAdminRequest.reloadCollection(COLLECTION).process(solrClient);
    new UpdateRequest()
        .add("id", "6", "text_syn", "humpty dumpy sat on a wall")
        .add("id", "7", "text_syn", "humpty dumpy3 sat on a walls")
        .add("id", "8", "text_syn", "humpty dumpy2 sat on a walled")
        .commit(solrClient, COLLECTION);
    final SolrQuery solrQuery2 = new SolrQuery("q", "text_syn:dumpy2", "rows", "0");
    QueryResponse response = solrClient.query(COLLECTION, solrQuery2);
    MatcherAssert.assertThat(response.getResults().getNumFound(), is(1L));
  }

}

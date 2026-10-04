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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that a "collection" parameter in the form body of a POST has its aliases resolved, and is
 * otherwise left as it is.
 */
public class AliasPostBodyTest extends SolrCloudTestCase {

  private static final String EMPTY_COLLECTION = "emptycoll";
  private static final String DOC_COLLECTION = "doccoll";
  private static final String EMPTY_ALIAS = "emptyalias";
  private static final String DOC_ALIAS = "docalias";
  private static final String BOTH_ALIAS = "bothalias";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();

    for (String collection : new String[] {EMPTY_COLLECTION, DOC_COLLECTION}) {
      CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(collection, 1, 1);
    }
    CollectionAdminRequest.createAlias(EMPTY_ALIAS, EMPTY_COLLECTION)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias(DOC_ALIAS, DOC_COLLECTION).process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias(BOTH_ALIAS, EMPTY_COLLECTION + "," + DOC_COLLECTION)
        .process(cluster.getSolrClient());
    new UpdateRequest().add("id", "1").commit(cluster.getSolrClient(), DOC_COLLECTION);
  }

  /** Sends a form body POST to the path alias, with the collection parameter in the body. */
  private static long postWithCollectionParam(String pathAlias, String collectionParam)
      throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("rows", "0");
    params.set("collection", collectionParam);
    SolrClient nodeClient = cluster.getJettySolrRunner(0).getSolrClient();
    QueryResponse response =
        new QueryRequest(params, SolrRequest.METHOD.POST).process(nodeClient, pathAlias);
    return response.getResults().getNumFound();
  }

  @Test
  public void testAliasInPostBodyIsResolved() throws Exception {
    assertEquals(0, postWithCollectionParam(EMPTY_ALIAS, EMPTY_ALIAS));
  }

  @Test
  public void testAliasInPostBodyDiffersFromThePathAlias() throws Exception {
    assertEquals(1, postWithCollectionParam(EMPTY_ALIAS, DOC_ALIAS));
  }

  @Test
  public void testCollectionInPostBodyIsNotReplacedByThePathCollection() throws Exception {
    assertEquals(1, postWithCollectionParam(EMPTY_ALIAS, DOC_COLLECTION));
  }

  @Test
  public void testBodyCollectionSurvivesTwoCollectionPathAlias() throws Exception {
    // The path alias resolves to two collections, so routing derives a two-collection
    // list for the request. The body's collection (the empty one) must still win: on
    // the base code it is overwritten with the path list and the document in the other
    // collection is counted too.
    assertEquals(0, postWithCollectionParam(BOTH_ALIAS, EMPTY_COLLECTION));
  }
}

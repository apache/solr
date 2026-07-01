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
package org.apache.solr.search;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Distributed search tests for the standard query parsers: {@code lucene}, {@code dismax}, and
 * {@code edismax}.
 */
public class DistributedQParserTest extends SolrCloudTestCase {

  private static final String COLLECTION = "distributed-qparser";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-dynamic")).configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2);

    new UpdateRequest()
        .add(sdoc("id", "1", "subject", "quick brown fox"))
        .add(sdoc("id", "2", "subject", "lazy brown dog"))
        .add(sdoc("id", "3", "subject", "quick red dog"))
        .add(sdoc("id", "4", "subject", "slow green cat"))
        .commit(cluster.getSolrClient(), COLLECTION);
  }

  @Test
  public void testLuceneQParser() throws Exception {
    QueryResponse response =
        new QueryRequest(params("q", "subject:quick", "defType", "lucene", "fl", "id"))
            .process(cluster.getSolrClient(), COLLECTION);
    assertEquals(2, response.getResults().getNumFound());

    response =
        new QueryRequest(params("q", "subject:brown", "defType", "lucene", "fl", "id"))
            .process(cluster.getSolrClient(), COLLECTION);
    assertEquals(2, response.getResults().getNumFound());
  }

  @Test
  public void testDismaxQParser() throws Exception {
    QueryResponse response =
        new QueryRequest(
                params("q", "quick", "defType", "dismax", "qf", "subject", "fl", "id"))
            .process(cluster.getSolrClient(), COLLECTION);
    assertEquals(2, response.getResults().getNumFound());

    response =
        new QueryRequest(
                params("q", "brown dog", "defType", "dismax", "qf", "subject", "fl", "id"))
            .process(cluster.getSolrClient(), COLLECTION);
    assertEquals(3, response.getResults().getNumFound());
  }

  @Test
  public void testEdismaxQParser() throws Exception {
    QueryResponse response =
        new QueryRequest(
                params("q", "quick", "defType", "edismax", "qf", "subject", "fl", "id"))
            .process(cluster.getSolrClient(), COLLECTION);
    assertEquals(2, response.getResults().getNumFound());

    response =
        new QueryRequest(
                params("q", "brown dog", "defType", "edismax", "qf", "subject", "fl", "id"))
            .process(cluster.getSolrClient(), COLLECTION);
    assertEquals(3, response.getResults().getNumFound());
  }
}

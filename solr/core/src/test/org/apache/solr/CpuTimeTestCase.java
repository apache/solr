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
package org.apache.solr;

import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ThreadStats;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that cpuTime is generated a
 */
public class CpuTimeTestCase extends SolrCloudTestCase {
  private static final String COLLECTION = "cpuTimeCollection1shard";
  private static final String COLLECTIONWITHSHARDS = "cpuTimeCollection2shard";
  private static final String id = "id";

  @BeforeClass
  public static void setupCluster() throws Exception {

    System.setProperty(ThreadStats.ENABLE_CPU_TIME, "true");
    
    // create and configure cluster
    configureCluster(2).addConfig("conf", configset("cloud-dynamic")).configure();

    // Tst with single shard and multiple shards collections
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(COLLECTIONWITHSHARDS, "conf", 2, 1)
        .process(cluster.getSolrClient());

    // add a document
    final SolrInputDocument doc1 =
        sdoc(
            id,
            "1",
            "title_s",
            "Here comes the sun",
            "artist_s",
            "The Beatles",
            "popularity_i",
            "123");
    new UpdateRequest().add(doc1).commit(cluster.getSolrClient(), COLLECTION);
    new UpdateRequest().add(doc1).commit(cluster.getSolrClient(), COLLECTIONWITHSHARDS);
  }

  @BeforeClass
  public static void clearSettings() throws Exception {
    System.clearProperty(ThreadStats.ENABLE_CPU_TIME);
  }
  
  @Test
  public void testCpuTimeInResponse() throws Exception {
    checkCpuTimeInResponseOnCollection(COLLECTION);
    checkCpuTimeInResponseOnCollection(COLLECTIONWITHSHARDS);
  }
  
  public void checkCpuTimeInResponseOnCollection(String collection) throws Exception {
    final SolrQuery solrQuery =
        new SolrQuery(
            "q", "*:*", "fl", "id,popularity_i", "sort", "popularity_i desc", "rows", "1");
    System.setProperty(ThreadStats.ENABLE_CPU_TIME, "true");

    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    
    final QueryResponse rsp = cloudSolrClient.query(collection, solrQuery);
    NamedList<Object> header = rsp.getHeader();
    List<Object> localCpuTimes = header.getAll(ThreadStats.LOCAL_CPU_TIME);
    List<Object> cpuTimes = header.getAll(ThreadStats.CPU_TIME);
    assertEquals("localCpuTime should not have values", 0, localCpuTimes.size());
    assertEquals("cpuTime should only have one value", 1, cpuTimes.size());
    long cpuTime = (long) cpuTimes.iterator().next();
    assertTrue("cpuTime (" + cpuTime + ") should be positive", cpuTime >= 0);
  }
}

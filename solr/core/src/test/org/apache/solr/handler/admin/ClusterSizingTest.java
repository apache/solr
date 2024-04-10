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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.api.model.ClusterSizingResponse;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SizeParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test {@link ClusterSizing} */
public class ClusterSizingTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = ClusterSizingTest.class.getSimpleName() + "_collection";
  private static final int NUM_DOCS = 2000;

  private static CloudSolrClient solrClient;
  private static CoreContainer coreContainer;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-cluster-sizing")).configure();
    solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2).process(solrClient);
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
    addDocs();
    coreContainer = cluster.getOpenOverseer().getCoreContainer();
    assumeWorkingMockito();
  }

  @AfterClass
  public static void releaseClient() {
    solrClient = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testClusterSizingV1() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(SizeParams.SIZE_UNIT, NumberUtils.sizeUnit.bytes.toString());
    params.add(SizeParams.ESTIMATION_RATIO, "10.0");
    params.add(CommonParams.WT, "xml");

    final SolrQueryRequest request = Mockito.mock(SolrQueryRequest.class);
    Mockito.when(request.getParams()).thenReturn(params);
    final SolrQueryResponse response = Mockito.mock(SolrQueryResponse.class);

    final NamedList<Object> values = new SimpleOrderedMap<>();

    final ClusterSizing sizing = new ClusterSizing(coreContainer, request, response);
    sizing.populate(values);

    if (log.isInfoEnabled()) {
      log.info("Result: {}", values);
    }
    // Validate values
    Assert.assertNotNull(values.get("cluster"));
    final NamedList<Object> cluster = (NamedList<Object>) values.get("cluster");

    Assert.assertNotNull(cluster.get("nodes"));
    final NamedList<Object> nodes = (NamedList<Object>) cluster.get("nodes");

    Assert.assertNotNull(cluster.get("collections"));
    final NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");

    final String nodeName = nodes.getName(0);
    final NamedList<Object> nodeInfo = (NamedList<Object>) nodes.get(nodeName);
    Assert.assertEquals(4, nodeInfo.size());

    final String collectionName = collections.getName(0);
    final NamedList<Object> collectionInfo = (NamedList<Object>) collections.get(collectionName);
    final String shardName = collectionInfo.getName(0);
    final NamedList<Object> shardInfo = (NamedList<Object>) collectionInfo.get(shardName);
    final String coreName = shardInfo.getName(0);
    final NamedList<Object> coreInfo = (NamedList<Object>) shardInfo.get(coreName);
    Assert.assertEquals(6, coreInfo.size());
    final NamedList<Object> solrDetails = (NamedList<Object>) coreInfo.get("solrDetails");
    Assert.assertEquals(4, solrDetails.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testClusterSizingV2() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(SizeParams.SIZE_UNIT, NumberUtils.sizeUnit.bytes.toString());
    params.add(SizeParams.ESTIMATION_RATIO, "10.0");

    final SolrQueryRequest request = Mockito.mock(SolrQueryRequest.class);
    Mockito.when(request.getParams()).thenReturn(params);

    final SolrQueryResponse response = new SolrQueryResponse();

    final ClusterSizing sizing = new ClusterSizing(coreContainer, request, response);
    final ClusterSizingResponse sizingResponse =
        sizing.estimateSize(
            0, 0, null, null, null, null, null, 0.0, NumberUtils.sizeUnit.bytes.toString());

    Assert.assertNotNull(sizingResponse);

    Assert.assertTrue(response.getValues().size() > 0);
    final NamedList<Object> values = response.getValues();
    Assert.assertNotNull(values.get("cluster"));
    final NamedList<Object> cluster = (NamedList<Object>) values.get("cluster");
    Assert.assertNotNull(cluster.get("nodes"));
    Assert.assertNotNull(cluster.get("collections"));

    if (log.isInfoEnabled()) {
      log.info("Response values: {}", response.getValues());
      log.info("ClusterSizingResponse cluster: {}", sizingResponse.cluster);
    }
  }

  private static void addDocs() throws Exception {
    final UpdateRequest update = new UpdateRequest();
    SolrInputDocument doc = null;
    for (int i = 0; i < NUM_DOCS; i++) {
      doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      doc.addField("text", TestUtil.randomAnalysisString(random(), 100, true));
      update.add(doc);
    }
    solrClient.request(update, COLLECTION);
    solrClient.commit(COLLECTION);

    final TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      final QueryResponse rsp = solrClient.query(COLLECTION, params("q", "*:*", "rows", "0"));
      if (rsp.getResults().getNumFound() == NUM_DOCS) {
        break;
      }
      timeOut.sleep(500);
    }
    assertFalse("timed out waiting for documents to be added", timeOut.hasTimedOut());
  }
}

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
package org.apache.solr.handler;

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RequestHandlerMetricsTest extends SolrCloudTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(1).addConfig("conf1", configset("cloud-aggregate-node-metrics")).configure();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    cluster.deleteAllCollections();
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("metricsEnabled");
  }

  @Test
  public void testAggregateNodeLevelMetrics() throws SolrServerException, IOException {
    String collection1 = "testRequestHandlerMetrics1";
    String collection2 = "testRequestHandlerMetrics2";

    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collection1, "conf1", 1, 1);
    cloudClient.request(create);
    cluster.waitForActiveCollection(collection1, 1, 1);

    create = CollectionAdminRequest.createCollection(collection2, "conf1", 1, 1);
    cloudClient.request(create);
    cluster.waitForActiveCollection(collection2, 1, 1);

    SolrInputDocument solrInputDocument =
        new SolrInputDocument("id", "10", "title", "test", "val_s1", "aaa");
    cloudClient.add(collection1, solrInputDocument);
    cloudClient.add(collection2, solrInputDocument);

    SolrQuery solrQuery = new SolrQuery("*:*");
    cloudClient.query(collection1, solrQuery);
    cloudClient.query(collection2, solrQuery);

    var coreContainer = cluster.getJettySolrRunners().get(0).getCoreContainer();

    try (SolrCore core1 = coreContainer.getCore(coreContainer.getAllCoreNames().get(0));
        SolrCore core2 = coreContainer.getCore(coreContainer.getAllCoreNames().get(1))) {

      CounterSnapshot.CounterDataPointSnapshot actualCore1Selects =
          SolrMetricTestUtils.newCloudSelectRequestsDatapoint(core1);
      CounterSnapshot.CounterDataPointSnapshot actualCore1Updates =
          SolrMetricTestUtils.newCloudUpdateRequestsDatapoint(core1);
      CounterSnapshot.CounterDataPointSnapshot actualCore2Selects =
          SolrMetricTestUtils.newCloudSelectRequestsDatapoint(core2);
      CounterSnapshot.CounterDataPointSnapshot actualCore2Updates =
          SolrMetricTestUtils.newCloudUpdateRequestsDatapoint(core2);
      CounterSnapshot.CounterDataPointSnapshot actualCore1SubmittedOps =
          SolrMetricTestUtils.getCounterDatapoint(
              core1,
              "solr_core_update_submitted_ops",
              SolrMetricTestUtils.newCloudLabelsBuilder(core1)
                  .label("category", "UPDATE")
                  .label("ops", "adds")
                  .build());
      CounterSnapshot.CounterDataPointSnapshot actualCore2SubmittedOps =
          SolrMetricTestUtils.getCounterDatapoint(
              core2,
              "solr_core_update_submitted_ops",
              SolrMetricTestUtils.newCloudLabelsBuilder(core2)
                  .label("category", "UPDATE")
                  .label("ops", "adds")
                  .build());

      assertEquals(1.0, actualCore1Selects.getValue(), 0.0);
      assertEquals(1.0, actualCore1Updates.getValue(), 0.0);
      assertEquals(1.0, actualCore2Updates.getValue(), 0.0);
      assertEquals(1.0, actualCore2Selects.getValue(), 0.0);
      assertEquals(1.0, actualCore1SubmittedOps.getValue(), 0.0);
      assertEquals(1.0, actualCore2SubmittedOps.getValue(), 0.0);

      // Get node metrics and the select/update requests should be the sum of both cores requests
      var nodeReader = SolrMetricTestUtils.getPrometheusMetricReader(coreContainer, "solr.node");

      CounterSnapshot.CounterDataPointSnapshot nodeSelectRequests =
          (CounterSnapshot.CounterDataPointSnapshot)
              SolrMetricTestUtils.getDataPointSnapshot(
                  nodeReader,
                  "solr_core_requests",
                  Labels.builder()
                      .label("category", "QUERY")
                      .label("handler", "/select")
                      .label("internal", "false")
                      .label("replica_type", "NRT")
                      .label("otel_scope_name", "org.apache.solr")
                      .build());
      CounterSnapshot.CounterDataPointSnapshot nodeUpdateRequests =
          (CounterSnapshot.CounterDataPointSnapshot)
              SolrMetricTestUtils.getDataPointSnapshot(
                  nodeReader,
                  "solr_core_requests",
                  Labels.builder()
                      .label("category", "UPDATE")
                      .label("handler", "/update")
                      .label("replica_type", "NRT")
                      .label("otel_scope_name", "org.apache.solr")
                      .build());
      CounterSnapshot.CounterDataPointSnapshot nodeSubmittedOps =
          (CounterSnapshot.CounterDataPointSnapshot)
              SolrMetricTestUtils.getDataPointSnapshot(
                  nodeReader,
                  "solr_core_update_submitted_ops",
                  Labels.builder()
                      .label("category", "UPDATE")
                      .label("ops", "adds")
                      .label("replica_type", "NRT")
                      .label("otel_scope_name", "org.apache.solr")
                      .build());

      assertNotNull("Node select requests should be recorded", nodeSelectRequests);
      assertNotNull("Node update requests should be recorded", nodeUpdateRequests);
      assertNotNull("Node submitted update operations should be recorded", nodeSubmittedOps);
      assertEquals(2.0, nodeSelectRequests.getValue(), 0.0);
      assertEquals(2.0, nodeUpdateRequests.getValue(), 0.0);
      assertEquals(2.0, nodeSubmittedOps.getValue(), 0.0);
    }
  }
}

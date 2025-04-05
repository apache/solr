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

import java.util.Locale;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEarlyTerminatingQueries extends SolrCloudTestCase {

  private static final String COLLECTION = "test";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    SolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 2);
    create.process(solrClient);
    waitForState("active", COLLECTION, clusterShape(3, 6));
    for (int j = 0; j < 100; j++) {
      solrClient.add(
          COLLECTION,
          sdoc(
              "id",
              "id-" + j,
              "val_i",
              j % 5,
              "text",
              TestUtil.randomHtmlishString(random(), 100)));
    }
    solrClient.commit(COLLECTION);
  }

  @Test
  public void testMaxHitsEarlyTermination() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();
    QueryResponse rsp = solrClient.query(COLLECTION, params("q", "*:*"));
    assertNull("should have full results", rsp.getHeader().get("partialResults"));

    rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                "maxHitsAllowed",
                "5",
                "rows",
                "5",
                "multiThreaded",
                "false"));
    assertEquals(
        "should have partial results for maxHitsAllowed",
        Boolean.TRUE,
        rsp.getHeader().get("partialResults"));
    assertEquals(
        "should have maxHitsTerminatedEarly response header for maxHitsAllowed",
        Boolean.TRUE,
        rsp.getHeader().get("maxHitsTerminatedEarly"));
    assertNotNull(
        "should have approximateTotalHits response header for maxHitsAllowed",
        rsp.getHeader().get("approximateTotalHits"));
    assertTrue(
        String.format(
            Locale.ROOT,
            "approximateTotalHits (%s) response header should be greater than numFound (%d)",
            rsp.getHeader().get("approximateTotalHits"),
            rsp.getResults().getNumFound()),
        ((Number) rsp.getHeader().get("approximateTotalHits")).longValue()
            > rsp.getResults().getNumFound());
  }
}

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

import java.util.Collection;
import java.util.Collections;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.servlet.CoordinatorHttpSolrCall;
import org.junit.BeforeClass;

public class TestCoordinatorRole extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
            .addConfig("conf", configset("cloud-minimal"))
            .configure();
  }

  public void testSimple() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    String COLLECTION_NAME = "test_coll";
    String SYNTHETIC_COLLECTION = CoordinatorHttpSolrCall.SYNTHETIC_COLL_PREFIX +"conf";
    CollectionAdminRequest
            .createCollection(COLLECTION_NAME, "conf", 2, 2)
            .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);
    UpdateRequest ur = new UpdateRequest();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc2 = new SolrInputDocument();
      doc2.addField("id", "" + i);
      doc2.addField("fld1_s", "1 value 1 value 1 value 1 value 1 value 1 value 1 value ");
      doc2.addField("fld2_s", "2 value 2 value 2 value 2 value 2 value 2 value 2 value 2 value 2 value 2 value ");
      doc2.addField("fld3_s", "3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value 3 value ");
      doc2.addField("fld4_s", "4 value 4 value 4 value 4 value 4 value 4 value 4 value 4 value 4 value ");
      doc2.addField("fld5_s", "5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value 5 value ");
      ur.add(doc2);
    }

    ur.commit(client, COLLECTION_NAME);
    QueryResponse rsp = client.query(COLLECTION_NAME, new SolrQuery("*:*"));
    assertEquals(10, rsp.getResults().getNumFound());

    System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
    JettySolrRunner coordinatorJetty = null;
    try {
      coordinatorJetty = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }
    NavigableObject result = (NavigableObject) Utils.executeGET(cluster.getSolrClient().getHttpClient(),
            coordinatorJetty.getBaseUrl().toString()+"/"+COLLECTION_NAME +"/select?q=*:*&wt=javabin", Utils.JAVABINCONSUMER);

    assertEquals(10, ((Collection)result._get("response", Collections.emptyList())).size());

    assertNotNull(cluster.getSolrClient().getClusterStateProvider().getCollection(SYNTHETIC_COLLECTION));

  }

}
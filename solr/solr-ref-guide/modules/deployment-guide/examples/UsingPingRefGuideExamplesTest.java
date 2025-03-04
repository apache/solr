/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example Ping usage.
 *
 * <p>Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference
 * Guide.
 */
public class UsingPingRefGuideExamplesTest extends SolrCloudTestCase {

  private static final int NUM_LIVE_NODES = 1;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    configureCluster(NUM_LIVE_NODES)
        .addConfig("conf", new File(ExternalPaths.TECHPRODUCTS_CONFIGSET).toPath())
        .configure();

    CollectionAdminResponse response =
        CollectionAdminRequest.createCollection("techproducts", "conf", 1, 1)
            .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("techproducts", 1, 1);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final SolrClient client = getSolrClient();

    final List<TechProduct> products = new ArrayList<>();
    products.add(new TechProduct("1", "Fitbit Alta"));
    products.add(new TechProduct("2", "Sony Walkman"));
    products.add(new TechProduct("3", "Garmin GPS"));

    client.addBeans("techproducts", products);
    client.commit("techproducts");
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    final SolrClient client = getSolrClient();
    client.deleteByQuery("techproducts", "*:*");
    client.commit("techproducts");
  }

  private SolrClient getSolrClient() {
    return cluster.getSolrClient();
  }

  @Test
  public void solrJExampleWithSolrPing() throws Exception {

    final SolrClient solrClient = getSolrClient();
    String collectionName = "techproducts";

    // tag::solrj-example-with-solrping[]
    SolrPing ping = new SolrPing();
    ping.getParams()
        .add("distrib", "true"); // To make it a distributed request against a collection
    SolrPingResponse rsp = ping.process(solrClient, collectionName);
    String status = (String) rsp.getResponse().get("status");
    // end::solrj-example-with-solrping[]

    assertEquals("OK", status);
  }

  @Test
  public void solrJExampleWithSolrClient() throws Exception {

    String collectionName = "techproducts";

    // tag::solrj-example-with-solrclient[]
    final SolrClient solrClient = getSolrClient();
    SolrPingResponse pingResponse = solrClient.ping(collectionName);
    String status = (String) pingResponse.getResponse().get("status");
    // end::solrj-example-with-solrclient[]

    assertEquals("OK", status);
  }

  public static class TechProduct {
    @Field public String id;
    @Field public String name;

    public TechProduct(String id, String name) {
      this.id = id;
      this.name = name;
    }

    public TechProduct() {}
  }
}

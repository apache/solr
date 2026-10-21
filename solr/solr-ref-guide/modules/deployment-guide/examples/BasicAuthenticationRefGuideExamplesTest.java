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
package org.apache.solr.client.ref_guide_examples;

import java.util.List;
import java.util.Optional;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example SolrJ usage with Basic Authentication.
 *
 * <p>Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference
 * Guide.
 */
public class BasicAuthenticationRefGuideExamplesTest extends SolrCloudAuthTestCase {

  private static final String COLLECTION = "techproducts";
  private static final String USER_NAME = "solr";
  private static final String PASSWORD = "SolrRocks";

  private static final String SECURITY_JSON =
      "{\n"
          + "  \"authentication\":{\n"
          + "   \"blockUnknown\": true,\n"
          + "   \"class\":\"solr.BasicAuthPlugin\",\n"
          + "   \"credentials\":{\"solr\":\"JeRyxP8A3dVWhFgFbf/Eg2RXmuoU8BE5gbNQyxmGAJQ= 6zkmjMjkMKyJX6/f0VarEWQujju5BzxZXub6WOrEKCw=\"}\n"
          + "  },\n"
          + "  \"authorization\":{\n"
          + "   \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n"
          + "   \"permissions\":[\n"
          + " {\"name\":\"security-edit\", \"role\":\"admin\"},\n"
          + " {\"name\":\"collection-admin-edit\", \"role\":\"admin\"},\n"
          + " {\"name\":\"core-admin-edit\", \"role\":\"admin\"}\n"
          + "   ],\n"
          + "   \"user-role\":{\"solr\":\"admin\"}\n"
          + "  }\n"
          + "}";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", ExternalPaths.DEFAULT_CONFIGSET)
        .withSecurityJson(SECURITY_JSON)
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .setBasicAuthCredentials(USER_NAME, PASSWORD)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    shutdownCluster();
  }

  @Test
  public void perClientHttpJettySolrClientExample() throws Exception {
    final String userName = USER_NAME;
    final String password = PASSWORD;
    final String solrUrl = cluster.getJettySolrRunner(0).getBaseUrl().toString() + "/" + COLLECTION;
    QueryRequest req = new QueryRequest(new SolrQuery("*:*"));

    // tag::solrj-basicauth-per-client-httpjettysolrclient[]
    var client =
        new HttpJettySolrClient.Builder(solrUrl)
            .withBasicAuthCredentials(userName, password)
            .build();
    QueryResponse rsp = req.process(client);
    // end::solrj-basicauth-per-client-httpjettysolrclient[]

    assertEquals(0, rsp.getStatus());
    client.close();
  }

  @Test
  public void perClientCloudSolrClientExample() throws Exception {
    final String userName = USER_NAME;
    final String password = PASSWORD;
    final List<String> zkHostList = List.of(cluster.getZkServer().getZkAddress());
    final Optional<String> chroot = Optional.empty();
    final SolrQuery query = new SolrQuery("*:*");
    query.set("collection", COLLECTION);
    QueryRequest req = new QueryRequest(query);

    // tag::solrj-basicauth-per-client-cloudsolrclient[]
    var httpClientBuilder =
        new HttpJettySolrClient.Builder().withBasicAuthCredentials(userName, password);
    var client =
        new CloudSolrClient.Builder(zkHostList, chroot)
            .withHttpClientBuilder(httpClientBuilder)
            .build();
    QueryResponse rsp = req.process(client);
    // end::solrj-basicauth-per-client-cloudsolrclient[]

    assertEquals(0, rsp.getStatus());
    client.close();
  }
}

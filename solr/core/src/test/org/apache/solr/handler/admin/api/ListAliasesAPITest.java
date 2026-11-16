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
package org.apache.solr.handler.admin.api;

import static org.hamcrest.CoreMatchers.containsString;

import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.model.GetAliasByNameResponse;
import org.apache.solr.client.api.model.ListAliasesResponse;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.AliasesApi;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/** HTTP integration tests for {@link ListAliases} using the generated SolrJ client. */
public class ListAliasesAPITest extends SolrCloudTestCase {
  private static final String COLLECTION_A = "listaliases_a";
  private static final String COLLECTION_B = "listaliases_b";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    for (String collection : List.of(COLLECTION_A, COLLECTION_B)) {
      CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(collection, 1, 1);
    }
  }

  @After
  public void clearAliases() throws Exception {
    var response = new AliasesApi.GetAliases().process(cluster.getSolrClient());
    for (String alias : response.aliases.keySet()) {
      new AliasesApi.DeleteAlias(alias).process(cluster.getSolrClient());
    }
  }

  @Test
  public void testGetAliases() throws Exception {
    createAliases();
    ListAliasesResponse response = new AliasesApi.GetAliases().process(cluster.getSolrClient());
    assertEquals(2, response.aliases.size());
    assertNotNull(response.responseHeader);
    assertEquals(List.of(COLLECTION_A), response.aliases.get("alias0").collections);
    assertEquals(List.of(COLLECTION_B, COLLECTION_A), response.aliases.get("alias1").collections);
    assertEquals(Map.of(), response.aliases.get("alias0").properties);
    assertEquals(Map.of("pkey1", "pvalA"), response.aliases.get("alias1").properties);
    for (String name : List.of("alias0", "alias1")) {
      GetAliasByNameResponse entry = response.aliases.get(name);
      GetAliasByNameResponse detail =
          new AliasesApi.GetAliasByName(name).process(cluster.getSolrClient());
      assertEquals(name, entry.alias);
      assertEquals(detail.collections, entry.collections);
      assertEquals(detail.properties, entry.properties);
    }
  }

  @Test
  public void testGetEmptyAliases() throws Exception {
    ListAliasesResponse response = new AliasesApi.GetAliases().process(cluster.getSolrClient());
    assertEquals(Map.of(), response.aliases);
    assertNotNull(response.responseHeader);
  }

  @Test
  public void testGetAliasByName() throws Exception {
    createAliases();
    GetAliasByNameResponse response =
        new AliasesApi.GetAliasByName("alias1").process(cluster.getSolrClient());
    assertEquals("alias1", response.alias);
    assertEquals(List.of(COLLECTION_B, COLLECTION_A), response.collections);
    assertEquals(Map.of("pkey1", "pvalA"), response.properties);
    assertNotNull(response.responseHeader);
  }

  @Test
  public void testGetMissingAliasByName() throws Exception {
    createAliases();
    var request = new AliasesApi.GetAliasByName("missingAlias");
    RemoteSolrException exception =
        expectThrows(RemoteSolrException.class, () -> request.process(cluster.getSolrClient()));
    assertEquals(404, exception.code());
    assertThat(exception.getMessage(), containsString("No such alias [missingAlias]"));
  }

  @Test
  public void testResponseFormatAndV1Compatibility() throws Exception {
    createAliases();
    var jetty = cluster.getJettySolrRunner(0);
    var httpResponse =
        jetty.getSolrClient().getHttpClient().newRequest(jetty.getBaseURLV2() + "/aliases").send();
    assertEquals(200, httpResponse.getStatus());
    Map<?, ?> listing = (Map<?, ?>) Utils.fromJSON(httpResponse.getContent());
    assertTrue(listing.containsKey("responseHeader"));
    assertFalse(listing.containsKey("properties"));
    assertEquals(
        Map.of(
            "alias0",
            Map.of("name", "alias0", "collections", List.of(COLLECTION_A), "properties", Map.of()),
            "alias1",
            Map.of(
                "name", "alias1",
                "collections", List.of(COLLECTION_B, COLLECTION_A),
                "properties", Map.of("pkey1", "pvalA"))),
        listing.get("aliases"));

    var legacyResponse = new CollectionAdminRequest.ListAliases().process(cluster.getSolrClient());
    assertEquals(
        Map.of("alias0", COLLECTION_A, "alias1", COLLECTION_B + "," + COLLECTION_A),
        legacyResponse.getAliases());
    assertEquals(Map.of("alias1", Map.of("pkey1", "pvalA")), legacyResponse.getAliasProperties());
  }

  private void createAliases() throws Exception {
    CollectionAdminRequest.createAlias("alias0", COLLECTION_A).process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias("alias1", COLLECTION_B + "," + COLLECTION_A)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.setAliasProperty("alias1")
        .addProperty("pkey1", "pvalA")
        .process(cluster.getSolrClient());
  }
}

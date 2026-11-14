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

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;

public class TechproductsV2IdlessUpdateTest extends SolrCloudTestCase {

  private static final String COLL = "tp";
  private static final Pattern UUID =
      Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig(COLL, ExternalPaths.TECHPRODUCTS_CONFIGSET).configure();
    CollectionAdminRequest.createCollection(COLL, COLL, 1, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLL, 1, 1);
  }

  public void testIdlessDocOnV2UpdateJsonGetsUuid() throws Exception {
    String body = "[{\"name\":\"v2-path\"}]";
    V2Request post =
        new V2Request.Builder("/collections/" + COLL + "/update/json")
            .withMethod(V2Request.METHOD.POST)
            .withParams(new ModifiableSolrParams().set("commit", "true"))
            .withPayload(body)
            .withMimeType("application/json")
            .build();
    Set<String> before = ids();
    try {
      var rsp = post.process(cluster.getSolrClient());
      System.out.println("REQ v2 ok body=" + rsp.getResponse());
    } catch (SolrException e) {
      System.out.println("REQ v2 http=" + e.code() + " err=" + e.getMessage());
    }
    verify("v2", before);
  }

  public void testIdlessDocOnUpdateJsonDocsGetsUuid() throws Exception {
    String body = "[{\"name\":\"docs-path\"}]";
    GenericSolrRequest post =
        new GenericSolrRequest(SolrRequest.METHOD.POST, "/update/json/docs")
            .setRequiresCollection(true)
            .withContent(body.getBytes(StandardCharsets.UTF_8), "application/json");
    Set<String> before = ids();
    try {
      var rsp = post.process(cluster.getSolrClient(), COLL);
      System.out.println("REQ docs ok body=" + rsp.getResponse());
    } catch (SolrException e) {
      System.out.println("REQ docs http=" + e.code() + " err=" + e.getMessage());
    }
    verify("docs", before);
  }

  private Set<String> ids() throws Exception {
    cluster.getSolrClient().commit(COLL);
    var q = new ModifiableSolrParams().set("q", "*:*").set("fl", "id").set("rows", "1000");
    QueryResponse rsp = cluster.getSolrClient().query(COLL, q);
    Set<String> out = new HashSet<>();
    for (SolrDocument d : rsp.getResults()) {
      out.add(String.valueOf(d.getFieldValue("id")));
    }
    return out;
  }

  private void verify(String label, Set<String> before) throws Exception {
    Set<String> added = ids();
    added.removeAll(before);
    System.out.println("ADDED " + label + " ids=" + added);
    assertEquals(label + " new documents", 1, added.size());
    String id = added.iterator().next();
    assertTrue(label + " id is not a UUID: " + id, UUID.matcher(id).matches());
  }
}

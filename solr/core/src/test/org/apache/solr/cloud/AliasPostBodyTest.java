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
package org.apache.solr.cloud;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that a collection alias in the POST form body is resolved the same as in the URL query
 * string (SOLR-12849).
 */
public class AliasPostBodyTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testAliasInPostBody() throws Exception {
    String collection = "testcoll";
    String alias = "testalias";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .processAndWait(cluster.getSolrClient(), 30);
    cluster.waitForActiveCollection(collection, 1, 1);
    CollectionAdminRequest.createAlias(alias, collection).process(cluster.getSolrClient());

    // POST to /solr/<alias>/select with collection=<alias> in the FORM BODY (ticket's scenario).
    // Must not fail with "Could not find collection".
    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    URL url = new URL(baseUrl + "/" + alias + "/select");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    String body = "q=*:*&rows=0&collection=" + alias;
    try (OutputStream os = conn.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
    }
    int code = conn.getResponseCode();
    String response = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertEquals(200, code);
    assertFalse(
        "POST with alias in body failed: " + response,
        response.contains("Could not find collection"));

    CollectionAdminRequest.deleteAlias(alias).process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }
}

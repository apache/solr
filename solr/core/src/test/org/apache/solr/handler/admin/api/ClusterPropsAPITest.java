/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin.api;

import static org.apache.solr.common.util.Utils.getObjectByPath;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

import java.net.URL;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClusterPropsAPITest extends SolrCloudTestCase {

  private URL baseUrl;
  private String baseUrlV2ClusterProps;

  private static final String testClusterProperty = "ext.test";
  private static final String testClusterPropertyValue = "test value";
  private static final String testClusterPropertyNestedKeyAndValue =
      "  \"defaults\": {"
          + "    \"collection\": {"
          + "      \"numShards\": 4,"
          + "      \"nrtReplicas\": 2,"
          + "      \"tlogReplicas\": 2,"
          + "      \"pullReplicas\": 2"
          + "    }"
          + "  }";
  private static final String testClusterPropertyBulkAndNestedValues =
      "{"
          + testClusterPropertyNestedKeyAndValue
          + ","
          + "  \""
          + testClusterProperty
          + "\": "
          + "\""
          + testClusterPropertyValue
          + "\""
          + " }";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    baseUrlV2ClusterProps =
        cluster.getJettySolrRunner(0).getBaseURLV2().toString() + "/cluster/properties";
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testClusterPropertyOpsAllGood() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      // List Properties, confirm the test property does not exist
      // This ignores eventually existing other properties
      Object o =
          Utils.executeGET(client.getHttpClient(), baseUrlV2ClusterProps, Utils.JSONCONSUMER);
      assertNotNull(o);
      @SuppressWarnings("unchecked")
      List<String> initProperties = (List<String>) getObjectByPath(o, true, "clusterProperties");
      assertThat(initProperties, not(hasItem(testClusterProperty)));

      // Create a single cluster property
      String path = baseUrlV2ClusterProps + "/" + testClusterProperty;
      HttpPut httpPut = new HttpPut(path);
      httpPut.setHeader("Content-Type", "application/json");
      httpPut.setEntity(new StringEntity("{\"value\":\"" + testClusterPropertyValue + "\"}"));
      o = Utils.executeHttpMethod(client.getHttpClient(), path, Utils.JSONCONSUMER, httpPut);
      assertNotNull(o);

      // List Properties, this time there should be the just added property
      o = Utils.executeGET(client.getHttpClient(), baseUrlV2ClusterProps, Utils.JSONCONSUMER);
      assertNotNull(o);
      @SuppressWarnings("unchecked")
      List<String> updatedProperties = (List<String>) getObjectByPath(o, true, "clusterProperties");
      assertThat(updatedProperties, hasItem(testClusterProperty));

      // Fetch Cluster Property
      // Same path as used in the Create step above
      o = Utils.executeGET(client.getHttpClient(), path, Utils.JSONCONSUMER);
      assertNotNull(o);
      assertEquals(testClusterProperty, (String) getObjectByPath(o, true, "clusterProperty/name"));
      assertEquals(
          testClusterPropertyValue, (String) getObjectByPath(o, true, "clusterProperty/value"));

      // Delete Cluster Property
      // Same path as used in the Create step above
      HttpDelete httpDelete = new HttpDelete(path);
      o = Utils.executeHttpMethod(client.getHttpClient(), path, Utils.JSONCONSUMER, httpDelete);
      assertNotNull(o);

      // List Properties, the test property should be gone
      o = Utils.executeGET(client.getHttpClient(), baseUrlV2ClusterProps, Utils.JSONCONSUMER);
      assertNotNull(o);
      @SuppressWarnings("unchecked")
      List<String> clearedProperties = (List<String>) getObjectByPath(o, true, "clusterProperties");
      assertThat(clearedProperties, not(hasItem(testClusterProperty)));
    }
  }

  @Test
  public void testClusterPropertyNestedBulkSet() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      // Create a single cluster property using the Bulk/Nested set ClusterProp API
      HttpPut httpPut = new HttpPut(baseUrlV2ClusterProps);
      httpPut.setHeader("Content-Type", "application/json");
      httpPut.setEntity(new StringEntity(testClusterPropertyBulkAndNestedValues));
      Object o =
          Utils.executeHttpMethod(
              client.getHttpClient(), baseUrlV2ClusterProps, Utils.JSONCONSUMER, httpPut);
      assertNotNull(o);

      // Fetch Cluster Property checking the not-nested property set above
      String path = baseUrlV2ClusterProps + "/" + testClusterProperty;
      o = Utils.executeGET(client.getHttpClient(), path, Utils.JSONCONSUMER);
      assertNotNull(o);
      assertEquals(testClusterProperty, (String) getObjectByPath(o, true, "clusterProperty/name"));
      assertEquals(
          testClusterPropertyValue, (String) getObjectByPath(o, true, "clusterProperty/value"));

      // Fetch Cluster Property checking the nested property set above
      path = baseUrlV2ClusterProps + "/" + "defaults";
      o = Utils.executeGET(client.getHttpClient(), path, Utils.JSONCONSUMER);
      assertNotNull(o);
      assertEquals("defaults", (String) getObjectByPath(o, true, "clusterProperty/name"));
      assertEquals(4L, getObjectByPath(o, true, "clusterProperty/value/collection/numShards"));

      // Clean up to leave the state unchanged
      HttpDelete httpDelete = new HttpDelete(path);
      Utils.executeHttpMethod(client.getHttpClient(), path, Utils.JSONCONSUMER, httpDelete);
      path = baseUrlV2ClusterProps + "/" + testClusterProperty;
      httpDelete = new HttpDelete(path);
      Utils.executeHttpMethod(client.getHttpClient(), path, Utils.JSONCONSUMER, httpDelete);
    }
  }

  @Test
  public void testClusterPropertyFetchNonExistentProperty() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      // Fetch Cluster Property that doesn't exist
      String path = baseUrlV2ClusterProps + "/ext.clusterPropThatDoesNotExist";
      HttpGet fetchClusterPropertyGet = new HttpGet(path);
      HttpResponse httpResponse = client.getHttpClient().execute(fetchClusterPropertyGet);
      assertEquals(404, httpResponse.getStatusLine().getStatusCode());
    }
  }
}

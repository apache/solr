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
package org.apache.solr.pkg;

import static org.apache.solr.filestore.TestDistribFileStore.uploadKey;

import java.net.URL;
import java.util.List;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.solr.client.solrj.apache.HttpClientUtil;
import org.apache.solr.client.solrj.apache.HttpSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.filestore.ClusterFileStore;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the JAX-RS-based {@link PackageAPIJaxRs}.
 *
 * <p>Note: SolrJettyTestRule cannot be used here because the Package API requires ZooKeeper for its
 * cluster-level operations. A one-node SolrCloud cluster is used instead.
 */
public class PackageAPIJaxRsTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.packages.enabled", "true");
    configureCluster(1)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testListPackagesReturnsEmptyResult() throws Exception {
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    String packageUrl =
        cluster.getJettySolrRunner(0).getBaseURLV2().toString() + "/cluster/package";

    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      Object response =
          HttpClientUtil.executeGET(client.getHttpClient(), packageUrl, Utils.JSONCONSUMER);
      assertNotNull("Expected non-null response from GET /cluster/package", response);
      // The response should have a 'result' field with 'packages' and 'znodeVersion'
      Object result = Utils.getObjectByPath(response, true, "result");
      assertNotNull("Expected 'result' field in GET /cluster/package response", result);
    }
  }

  @Test
  public void testAddDeletePackageVersion() throws Exception {
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    String baseUrlV2 = cluster.getJettySolrRunner(0).getBaseURLV2().toString();
    String FILE1 = "/testpkg/runtimelibs.jar";

    // Upload a key and a signed jar file to the filestore
    byte[] derFile =
        org.apache.solr.filestore.TestDistribFileStore.readFile("cryptokeys/pub_key512.der");
    uploadKey(derFile, ClusterFileStore.KEYS_DIR + "/pub_key512.der", cluster);
    org.apache.solr.pkg.TestPackages.postFileAndWait(
        cluster,
        "runtimecode/runtimelibs.jar.bin",
        FILE1,
        "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ==");

    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      // Add a package version via POST /cluster/package/{name}/versions
      String addUrl = baseUrlV2 + "/cluster/package/testpkg/versions";
      HttpPost httpPost = new HttpPost(addUrl);
      httpPost.setHeader("Content-Type", "application/json");
      httpPost.setEntity(new StringEntity("{\"version\":\"1.0\",\"files\":[\"" + FILE1 + "\"]}"));
      Object addResponse =
          HttpClientUtil.executeHttpMethod(
              client.getHttpClient(), addUrl, Utils.JSONCONSUMER, httpPost);
      assertNotNull(
          "Expected non-null response from POST /cluster/package/testpkg/versions", addResponse);

      // Verify the package was added via GET /cluster/package
      String listUrl = baseUrlV2 + "/cluster/package";
      Object listResponse =
          HttpClientUtil.executeGET(client.getHttpClient(), listUrl, Utils.JSONCONSUMER);
      assertNotNull(listResponse);

      // Verify the package appears in the list
      @SuppressWarnings("unchecked")
      List<Object> versions =
          (List<Object>) Utils.getObjectByPath(listResponse, true, "result/packages/testpkg");
      assertNotNull("Expected testpkg to be present in packages", versions);
      assertFalse("Expected at least one version", versions.isEmpty());

      // Verify GET /cluster/package/{name} returns only this package
      String getByNameUrl = baseUrlV2 + "/cluster/package/testpkg";
      Object getByNameResponse =
          HttpClientUtil.executeGET(client.getHttpClient(), getByNameUrl, Utils.JSONCONSUMER);
      assertNotNull(getByNameResponse);
      @SuppressWarnings("unchecked")
      List<Object> versionsFromGet =
          (List<Object>) Utils.getObjectByPath(getByNameResponse, true, "result/packages/testpkg");
      assertNotNull("Expected testpkg in GET by name response", versionsFromGet);

      // Delete the package version via DELETE /cluster/package/{name}/versions/{version}
      String deleteUrl = baseUrlV2 + "/cluster/package/testpkg/versions/1.0";
      HttpDelete httpDelete = new HttpDelete(deleteUrl);
      Object deleteResponse =
          HttpClientUtil.executeHttpMethod(
              client.getHttpClient(), deleteUrl, Utils.JSONCONSUMER, httpDelete);
      assertNotNull("Expected non-null response from DELETE", deleteResponse);
    }
  }

  @Test
  public void testAddPackageVersionValidatesFiles() throws Exception {
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    String baseUrlV2 = cluster.getJettySolrRunner(0).getBaseURLV2().toString();

    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      // Try to add a package version with a non-existent file
      String addUrl = baseUrlV2 + "/cluster/package/testpkg2/versions";
      HttpPost httpPost = new HttpPost(addUrl);
      httpPost.setHeader("Content-Type", "application/json");
      httpPost.setEntity(
          new StringEntity("{\"version\":\"1.0\",\"files\":[\"/nonexistent/file.jar\"]}"));

      org.apache.http.HttpResponse httpResponse = client.getHttpClient().execute(httpPost);
      int statusCode = httpResponse.getStatusLine().getStatusCode();
      assertEquals("Expected 400 BAD_REQUEST when specifying non-existent file", 400, statusCode);
    }
  }

  @Test
  public void testRefreshNonExistentPackage() throws Exception {
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    String baseUrlV2 = cluster.getJettySolrRunner(0).getBaseURLV2().toString();

    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      // Try to refresh a non-existent package
      String refreshUrl = baseUrlV2 + "/cluster/package/nonexistentpkg/refresh";
      HttpPost httpPost = new HttpPost(refreshUrl);
      httpPost.setHeader("Content-Type", "application/json");

      org.apache.http.HttpResponse httpResponse = client.getHttpClient().execute(httpPost);
      int statusCode = httpResponse.getStatusLine().getStatusCode();
      assertEquals(
          "Expected 400 BAD_REQUEST when refreshing non-existent package", 400, statusCode);
    }
  }
}

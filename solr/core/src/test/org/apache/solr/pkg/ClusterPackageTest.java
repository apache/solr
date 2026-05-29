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

import java.util.List;
import org.apache.solr.client.api.model.PackagesResponse;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.PackageApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.filestore.ClusterFileStore;
import org.apache.solr.filestore.TestDistribFileStore;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the JAX-RS-based {@link ClusterPackage}.
 *
 * <p>Note: SolrJettyTestRule cannot be used here because the Package API requires ZooKeeper for its
 * cluster-level operations. A one-node SolrCloud cluster is used instead.
 */
public class ClusterPackageTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.packages.enabled", "true");
    configureCluster(1)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testListPackagesReturnsResult() throws Exception {
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
            .build()) {
      PackagesResponse response = new PackageApi.ListPackages().process(client);
      assertNotNull("Expected non-null response from GET /cluster/package", response);
      assertNotNull("Expected 'result' field in GET /cluster/package response", response.result);
    }
  }

  @Test
  public void testAddAndDeletePackageVersion() throws Exception {
    String FILE1 = "/pkgapitestpkg/runtimelibs.jar";

    // Upload a key and a signed jar file to the filestore
    byte[] derFile = TestDistribFileStore.readFile("cryptokeys/pub_key512.der");
    uploadKey(derFile, ClusterFileStore.KEYS_DIR + "/pub_key512.der", cluster);
    TestDistribFileStore.postFileAndWait(
        cluster,
        "runtimecode/runtimelibs.jar.bin",
        FILE1,
        "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ==");

    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
            .build()) {
      // Add a package version via POST /cluster/package/{name}/versions
      PackageApi.AddPackageVersion addRequest = new PackageApi.AddPackageVersion("pkgapitestpkg");
      addRequest.setVersion("1.0");
      addRequest.setFiles(List.of(FILE1));
      addRequest.process(client);

      // Verify the package was added via GET /cluster/package
      PackagesResponse listResponse = new PackageApi.ListPackages().process(client);
      assertNotNull("Expected non-null list response", listResponse);
      assertNotNull("Expected non-null result", listResponse.result);
      assertNotNull(
          "Expected pkgapitestpkg in packages", listResponse.result.packages.get("pkgapitestpkg"));
      assertFalse(
          "Expected at least one version",
          listResponse.result.packages.get("pkgapitestpkg").isEmpty());

      // Verify GET /cluster/package/{name} returns only this package
      PackagesResponse getByNameResponse =
          new PackageApi.GetPackage("pkgapitestpkg").process(client);
      assertNotNull("Expected non-null get-by-name response", getByNameResponse);
      assertNotNull("Expected non-null result from get-by-name", getByNameResponse.result);
      assertNotNull(
          "Expected pkgapitestpkg in get-by-name response",
          getByNameResponse.result.packages.get("pkgapitestpkg"));

      // Delete the package version via DELETE /cluster/package/{name}/versions/{version}
      new PackageApi.DeletePackageVersion("pkgapitestpkg", "1.0").process(client);

      // Verify it's deleted
      PackagesResponse listAfterDelete = new PackageApi.ListPackages().process(client);
      assertNotNull("Expected non-null list response after delete", listAfterDelete);
      assertNotNull("Expected non-null result after delete", listAfterDelete.result);
      // After deleting the only version, the package entry should be empty or absent
      List<?> versionsAfterDelete = listAfterDelete.result.packages.get("pkgapitestpkg");
      assertTrue(
          "Expected no versions after delete",
          versionsAfterDelete == null || versionsAfterDelete.isEmpty());
    }
  }

  @Test
  public void testAddPackageVersionValidatesFiles() {
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
            .build()) {
      PackageApi.AddPackageVersion addRequest = new PackageApi.AddPackageVersion("testpkg_invalid");
      addRequest.setVersion("1.0");
      addRequest.setFiles(List.of("/nonexistent/file.jar"));

      RemoteSolrException ex =
          expectThrows(RemoteSolrException.class, () -> addRequest.process(client));
      assertEquals("Expected 400 for non-existent file", 400, ex.code());
      assertTrue(
          "Expected error message to mention the file: " + ex.getMessage(),
          ex.getMessage().contains("No such file"));
    }
  }

  @Test
  public void testRefreshNonExistentPackage() {
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
            .build()) {
      RemoteSolrException ex =
          expectThrows(
              RemoteSolrException.class,
              () -> new PackageApi.RefreshPackage("nonexistentpkg_test").process(client));
      assertEquals("Expected 400 for non-existent package", 400, ex.code());
      assertTrue(
          "Expected error message to mention the package: " + ex.getMessage(),
          ex.getMessage().contains("No such package"));
    }
  }

  @Test
  public void testListPackagesAcceptsRefreshPackageQueryParam() throws Exception {
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
            .build()) {
      PackageApi.ListPackages req = new PackageApi.ListPackages();
      req.setRefreshPackage("nonexistentpkg_refresh");
      // The inter-node refresh signal calls notifyListeners on this node and returns OK even when
      // the package doesn't exist; main's behavior is identical.
      PackagesResponse response = req.process(client);
      assertNotNull(response);
    }
  }

  @Test
  public void testListAndGetPackageAcceptExpectedVersionQueryParam() throws Exception {
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
            .build()) {
      // expectedVersion at-or-below current is a no-op in syncToVersion and must return promptly.
      PackageApi.ListPackages listReq = new PackageApi.ListPackages();
      listReq.setExpectedVersion(0);
      PackagesResponse listResponse = listReq.process(client);
      assertNotNull(listResponse);
      assertNotNull(listResponse.result);

      PackageApi.GetPackage getReq = new PackageApi.GetPackage("anything");
      getReq.setExpectedVersion(0);
      PackagesResponse getResponse = getReq.process(client);
      assertNotNull(getResponse);
      assertNotNull(getResponse.result);
    }
  }
}

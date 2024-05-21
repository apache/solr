/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.pkg;

import static org.apache.solr.filestore.TestDistribFileStore.uploadKey;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.filestore.FileStoreAPI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PackageVersionNotHonoredTest extends SolrCloudTestCase {

  private static SolrClient client;

  private static final KeyPair KEY_PAIR;

  static {
    try {
      KEY_PAIR = KeyPairGenerator.getInstance("RSA").generateKeyPair();
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("should not happen", e);
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("enable.packages", "true");
    configureCluster(2)
        // add a configset where one schema field is of type `my.pkg.MyTextField`
        // this class is available via schema-plugins.jar.bin
        .addConfig("conf", configset("conf-using-mypkg-version-1"))
        .configure();

    client = cluster.getSolrClient();
    uploadKey(KEY_PAIR.getPublic().getEncoded(), FileStoreAPI.KEYS_DIR + "/pub.der", cluster);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    System.clearProperty("enable.packages");
    super.tearDown();
  }

  @Test
  public void testPackageVersions() throws Exception {
    // register schema-plugins.jar.bin as package mypkg (version 1)
    // this is the jar that has necessary class `my.pkg.MyTextField`,
    // and we expect our configset to load this jar because of the
    // constraint in params.json of the configset
    uploadPluginJar("1", getFile("runtimecode/schema-plugins.jar.bin").toPath());
    registerPackage("1");

    // register some other jar as package mypkg (version 2)
    // this jar does NOT include `my.pkg.MyTextField` which
    // shouldn't matter because the configset does not
    // reference this version of the package
    uploadPluginJar("2", getFile("runtimecode/runtimelibs.jar.bin").toPath());
    registerPackage("2");

    // create a collection that uses configset `conf`
    // which references package mypkg (version 1)
    createCollection();

    // without the fix, the test fails and the logs indicate that
    // mypkg version 2 is used, and not version 1 which was requested
    //
    // > Caused by: org.apache.solr.common.SolrException: PACKAGE_LOADER:
    // mypkg:{"package":"mypkg","version":"2","files":["/my-plugin-2/plugin.jar"]} Error loading
    // class 'my.pkg.MyTextField'
  }

  // utility methods

  private static String signature(byte[] content) throws Exception {
    Signature signature = Signature.getInstance("SHA1WithRSA");

    signature.initSign(KEY_PAIR.getPrivate());
    signature.update(content);
    byte[] result = signature.sign();

    return Base64.getEncoder().encodeToString(result);
  }

  private void uploadPluginJar(String version, Path jarPath) throws Exception {
    var pluginRequest =
        new V2Request.Builder("/cluster/files/my-plugin-" + version + "/plugin.jar")
            .PUT()
            .withParams(params("sig", signature(Files.readAllBytes(jarPath))))
            .withPayload(Files.newInputStream(jarPath))
            .forceV2(true)
            .build();
    processRequest(client, pluginRequest);
  }

  private void registerPackage(String version) throws Exception {
    var packageRequest =
        new V2Request.Builder("/cluster/package")
            .POST()
            .forceV2(true)
            .withPayload(
                Map.of(
                    "add",
                    Map.of(
                        "package",
                        "mypkg",
                        "version",
                        version,
                        "files",
                        List.of("/my-plugin-" + version + "/plugin.jar"))))
            .build();
    processRequest(client, packageRequest);
  }

  private void createCollection() throws Exception {
    var createRequest = CollectionAdminRequest.createCollection("coll", "conf", 1, 1);
    processRequest(client, createRequest);
  }

  private static void processRequest(SolrClient client, SolrRequest<?> request) throws Exception {
    var response = request.process(client);
    if (response.getException() != null) {
      throw response.getException();
    }
  }
}

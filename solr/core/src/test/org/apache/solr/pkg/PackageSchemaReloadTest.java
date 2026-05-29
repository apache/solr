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

import static org.apache.solr.filestore.TestDistribFileStore.postFileAndWait;
import static org.apache.solr.filestore.TestDistribFileStore.readFile;
import static org.apache.solr.filestore.TestDistribFileStore.uploadKey;

import java.util.Arrays;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.PackageApi;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.filestore.ClusterFileStore;
import org.apache.solr.filestore.TestDistribFileStore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that bumping a package version that backs a schema plugin causes the affected core's
 * schema to reload and pick up classes from the new package's classloader.
 */
public class PackageSchemaReloadTest extends SolrCloudTestCase {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.packages.enabled", "true");
    configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .addConfig("conf1", configset("schema-package"))
        .configure();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    super.tearDown();
  }

  @Test
  public void testSchemaReloadOnPackageVersionBump() throws Exception {
    String COLLECTION_NAME = "testSchemaLoadingColl";
    System.setProperty("managed.schema.mutable", "true");

    IndexSchema[] schemas = new IndexSchema[2]; // tracks schemas for a selected core

    String FILE1 = "/schemapkg/schema-plugins.jar";
    byte[] derFile = readFile("cryptokeys/pub_key512.der");
    uploadKey(derFile, ClusterFileStore.KEYS_DIR + "/pub_key512.der", cluster);
    postFileAndWait(
        cluster,
        "runtimecode/schema-plugins.jar.bin",
        FILE1,
        "U+AdO/jgY3DtMpeFRGoTQk72iA5g/qjPvdQYPGBaXB5+ggcTZk4FoIWiueB0bwGJ8Mg3V/elxOqEbD2JR8R0tA==");

    String FILE2 = "/schemapkg/payload-component.jar";
    postFileAndWait(
        cluster,
        "runtimecode/payload-component.jar.bin",
        FILE2,
        "gI6vYUDmSXSXmpNEeK1cwqrp4qTeVQgizGQkd8A4Prx2K8k7c5QlXbcs4lxFAAbbdXz9F4esBqTCiLMjVDHJ5Q==");

    // upload package v1.0
    PackageApi.AddPackageVersion addReq = new PackageApi.AddPackageVersion("schemapkg");
    addReq.setVersion("1.0");
    addReq.setFiles(Arrays.asList(FILE1, FILE2));
    addReq.process(cluster.getSolrClient());

    TestDistribFileStore.assertResponseValues(
        10,
        () ->
            new V2Request.Builder("/cluster/package")
                .withMethod(SolrRequest.METHOD.GET)
                .build()
                .process(cluster.getSolrClient()),
        Map.of(
            ":result:packages:schemapkg[0]:version",
            "1.0",
            ":result:packages:schemapkg[0]:files[0]",
            FILE1));

    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1", 2, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);

    // make note of the schema instance for one of the cores
    SolrCore.Provider coreProvider =
        cluster.getJettySolrRunners().stream()
            .flatMap(
                jetty ->
                    jetty.getCoreContainer().getAllCoreNames().stream()
                        .map(name -> new SolrCore.Provider(jetty.getCoreContainer(), name, null)))
            .findFirst()
            .orElseThrow();

    coreProvider.withCore(core -> schemas[0] = core.getLatestSchema());

    // upload package v2.0
    addReq.setVersion("2.0");
    addReq.setFiles(Arrays.asList(FILE1, FILE2));
    addReq.process(cluster.getSolrClient());

    TestDistribFileStore.assertResponseValues(
        10,
        () ->
            new V2Request.Builder("/cluster/package")
                .withMethod(SolrRequest.METHOD.GET)
                .build()
                .process(cluster.getSolrClient()),
        Map.of(
            ":result:packages:schemapkg[1]:version",
            "2.0",
            ":result:packages:schemapkg[1]:files[0]",
            FILE1));

    // even though package version 2.0 uses exactly the same files
    // as version 1.0, the core schema should still reload, and
    // the core should be associated with a different schema instance
    TestDistribFileStore.assertResponseValues(
        10,
        () -> {
          coreProvider.withCore(core -> schemas[1] = core.getLatestSchema());
          return params("schemaReloaded", (schemas[0] != schemas[1]) ? "yes" : "no");
        },
        Map.of("schemaReloaded", "yes"));

    // after the reload, the custom field type class now comes from package v2.0
    String fieldTypeName = "myNewTextFieldWithAnalyzerClass";

    FieldType fieldTypeV1 = schemas[0].getFieldTypeByName(fieldTypeName);
    assertEquals("my.pkg.MyTextField", fieldTypeV1.getClass().getCanonicalName());

    FieldType fieldTypeV2 = schemas[1].getFieldTypeByName(fieldTypeName);
    assertEquals("my.pkg.MyTextField", fieldTypeV2.getClass().getCanonicalName());

    assertNotEquals(
        "my.pkg.MyTextField classes should be from different classloaders",
        fieldTypeV1.getClass(),
        fieldTypeV2.getClass());
  }
}

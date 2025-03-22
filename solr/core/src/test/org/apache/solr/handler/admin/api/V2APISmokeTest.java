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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class V2APISmokeTest extends SolrCloudTestCase {

    // TODO: How is this normally done in these tests?
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private URL baseUrl;
    private String baseUrlV2;

    @BeforeClass
    public static void setupCluster() throws Exception {
        System.setProperty("enable.packages", "true"); // for file upload
        System.setProperty("solr.allowPaths", "*"); // for backups
        configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
        baseUrlV2 = cluster.getJettySolrRunner(0).getBaseURLV2().toString();
    }

    @AfterClass
    public static void teardownClass() {
        System.clearProperty("enable.packages");
        System.clearProperty("solr.allowPaths");
    }

    @Test
    public void testCollectionsApi() throws Exception {
        canGet("/collections");
        canPost("/collections", """
                {
                  "name": "testCollection",
                  "numShards": 1,
                  "nrtReplicas": 2
                }
                """);

        final String collectionPath = "/collections/testCollection";
        canGet(collectionPath);
//        canGet(collectionPath+"-not-a-collection"); // TODO returns 500, should be 404

        canPut(collectionPath + "/properties/foo", """
                {
                  "value": "bar"
                }
                """);
        canDelete(collectionPath + "/properties/foo");

        canPost(collectionPath + "/balance-shard-unique", """
                {
                  "property": "preferredLeader"
                }
                """);

        Path v2apiBackupPath = createTempDir("v2apiBackup");
        canPost(collectionPath + "/backups/testBackup/versions", String.format(Locale.ROOT, """
                {
                    "location": "file:///%s"
                }
                """, v2apiBackupPath.toString().replace("\\", "/")));

        canPost(collectionPath + "/reload", "{}");

        // TODO: is there a better API to GET the list of shards for a collection rather than the cluster status?
        // Or use implict and create / delete
        String shardName = canGet("/cluster", TestClusterResponseStub.class).cluster.collections.get("testCollection").shards.keySet().stream().findAny().get();

//        canPost(collectionPath + "/shards", """
//                {
//                    "name": "s1"
//                }
//                """);
        String shardPath = collectionPath + "/shards/" + shardName;
//        canPost(shardPath + "/force-leader", "{}"); // TODO: 500 - The shard already has an active leader. Force leader is not applicable.
        canPost(shardPath + "/replicas", "{}");
//        canDelete(shardPath + "/replicas/{replicaName}");
//        canDelete(shardPath + "/replicas");
//        canPost(shardPath + "/sync", "{}"); // TODO: 500 - Sync Failed
//        canPost(shardPath + "/install", """
//                {
//                    "location": "TODO"
//                }
//                """);
//        canPut(shardPath + "/replicas/{replicaName}/properties/{propName}");
//        canDelete(shardPath + "/replicas/{replicaName}/properties/{propName}");
//        canDelete(shardPath);

        canGet(collectionPath + "/snapshots");
        //canPost(collectionPath + "/snapshots/snap123", "{}"); // TODO expected:<200> but was:<405>
        canDelete(collectionPath + "/snapshots/snap123");

        testCollectionsAndCoresApi("collections", "testCollection");

        canPut(collectionPath + "/scale", """
                {
                    "count" : 1
                }
                """);
        // TODO: This randomly fails, need to wait for something I guess...
        // java.lang.NullPointerException: Cannot invoke "org.apache.solr.request.SolrQueryRequest.getRequestTimer()" because "solrQueryRequest" is null
        //	at org.apache.solr.jersey.PostRequestDecorationFilter.filter(PostRequestDecorationFilter.java:65)
        //	at org.glassfish.jersey.server.ContainerFilteringStage$ResponseFilterStage.apply(ContainerFilteringStage.java:172)
        canPost(collectionPath + "/rename", """
                {
                  "to": "test123"
                }
                """);
        canDelete("/aliases/test123");
        canDelete(collectionPath);
    }

    @Test
    public void testAliasesApi() throws Exception {
        canPost("/collections", """
                {
                  "name": "aCollection",
                  "numShards": 1
                }
                """);

        canGet("/aliases");
        canPost("/aliases", """
                {
                  "name": "foo",
                  "collections": ["aCollection"]
                }
                """);
        //canGet("/aliases/foo"); // TODO@ BUG = 405 - GET is hidden by overloaded @Path...
        canGet("/aliases/foo/properties");
        canPut("/aliases/foo/properties", """
                {
                  "properties":
                   {
                     "bar": "car"
                   }
                }
                """);

        canGet("/aliases/foo/properties/bar");
        canPut("/aliases/foo/properties/bar", """
                {}
                """);
        canDelete("/aliases/foo/properties/bar");
        canDelete("/aliases/foo");
    }

    @Test
    public void testBackupsApi() throws Exception {
        // TODO: Need to create backup
//        canPut("/backups/{backupName}/purgeUnused", "{}");
//        canPost("/backups/{backupName}/restore", "{}");
//        canGet("/backups/{backupName}/versions");
//        canDelete("/backups/{backupName}/versions");
//        canDelete("/backups/{backupName}/versions/{backupId}");
    }

    @Test
    public void testClusterApi() throws Exception {
        canGet("/cluster"); // TODO: Missing from OA spec
        String testFile = "testFile-" + Instant.now().toEpochMilli();
        String testFilePath = "/cluster/filestore/files/" + testFile;
        canPut(testFilePath, createTempFile().toFile());
        canGet(testFilePath);
        canGet("/cluster/filestore/metadata/" + testFile);
//        canDelete(testFilePath); // TODO: 500 because delete cluster file calls delete local, which fails if file exists in cluster
        canPost("/cluster/filestore/commands/fetch/" + testFile, "{}");
//        canPost("/cluster/filestore/commands/sync/" + testFile, "{}"); // TODO: 500 - node already exists in ZK, how can this API be used?

        canGet("/cluster/properties");
        canPut("/cluster/properties", """
                {
                    "ext.foo": "bar",
                    "ext.foo2": "bar2"
                }
                """);
        canGet("/cluster/properties/ext.foo");
        canPut("/cluster/properties/ext.foo", """
                {
                    "value": "car"
                }
                """);
        canDelete("/cluster/properties/ext.foo");

        canGet("/cluster/zookeeper/children/collections");
        canGet("/cluster/zookeeper/data/clusterprops.json");
        canGet("/cluster/zookeeper/data/security.json");

        canPost("/cluster/replicas/balance", "{}");
        String nodeName = getNodeName();
        canPost("/cluster/nodes/" + nodeName + "/clear", "");
        canPost("/cluster/nodes/" + nodeName + "/replace", "");
        canPost("/cluster/replicas/migrate", String.format(Locale.ROOT, """
                {
                    "sourceNodes": ["%s"]
                }
                """, nodeName));
    }

    private String getNodeName() throws Exception {
        TestNodesResponseStub nodesResponse = canGet("/cluster/nodes", TestNodesResponseStub.class); // TODO: Missing from OA Spec
        return nodesResponse.nodes.get(0);
    }

    @Test
    public void testConfigSetsApi() throws Exception {
        canGet("/configsets");
        File zipFile = createTempZipFile("solr/configsets/upload/regular");
        canPut("/configsets/cfg123", zipFile);
        canPut("/configsets/cfg123/file345", """
                """);
        canPost("/configsets", """
                {
                  "name": "cfg456",
                  "baseConfigSet": "cfg123"
                }
                """);
        canDelete("/configsets/cfg123");
    }

    @Test
    public void testCoresApi() throws Exception {
        canPost("/collections", """
                {
                  "name": "testCore",
                  "numShards": 1
                }
                """);
        canGet("/cores");

//        final String nodeNode = getNodeName();
        // Unable to create core [testCore] Caused by: No shard id for CoreDescriptor
//        canPost("/cores", String.format(Locale.ROOT, """
//                  {
//                      "name": "testCore",
//                      "coreNodeName": "%s",
//                      "configSet": "_default"
//                  }
//                """, nodeNode));
//        String corePath = "/cores/testCore";
//        canGet(corePath);
//        canPost(corePath + "/reload", """
//                """);
//        canPost(corePath + "/backups");
//        canPost(corePath + "/install");
//        canPost(corePath + "/merge-indices");
//        canPost(corePath + "/rename");
//        canPost(corePath + "/replication/backups");
//        canGet(corePath + "/replication/files");
//        canGet(corePath + "/replication/files/{filePath}");
//        canGet(corePath + "/replication/indexversion");
//        canPost(corePath + "/restore");
//        canGet(corePath + "/segments");
//        canGet(corePath + "/snapshots");
//        canPost(corePath + "/snapshots/{snapshotName}");
//        canDelete(corePath + "/snapshots/{snapshotName}");
//        canPost(corePath + "/swap");
//        canPost(corePath + "/unload");

        testCollectionsAndCoresApi("cores", "testCore");
    }


    @Test
    public void testNodesApi() throws Exception {
        canGet("/node/key");
        canGet("/node/logging/levels");
        canPut("/node/logging/levels", "[]");
        canGet("/node/logging/messages?since=1739116089");
        canPut("/node/logging/messages/threshold", """
                {
                  "level": "WARN"
                }
                """);
        canGet("/node/commands/123");
    }

    private void testCollectionsAndCoresApi(String indexType, String indexName) throws Exception {
        //indexType = collections | cores
        String indexPath = "/" + indexType + "/" + indexName;
        String schemaPath = indexPath + "/schema";
        canGet(schemaPath);
        canGet(schemaPath + "/copyfields");
        canGet(schemaPath + "/dynamicfields");
        canGet(schemaPath + "/dynamicfields/*_txt_en");
        canGet(schemaPath + "/fields");
        canGet(schemaPath + "/fields/id");
        canGet(schemaPath + "/fieldtypes");
        canGet(schemaPath + "/fieldtypes/boolean");
        canGet(schemaPath + "/name");
        canGet(schemaPath + "/similarity");
        canGet(schemaPath + "/uniquekey");
        canGet(schemaPath + "/version");
        canGet(schemaPath + "/zkversion");
        canGet(indexPath + "/select");
        canPost(indexPath + "/select", "{}");
    }


    private void canPost(String url, String content) throws Exception {
        try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
            HttpPost httpPost = new HttpPost(baseUrlV2 + url);
            httpPost.setEntity(
                    new StringEntity(content, ContentType.create("application/json", StandardCharsets.UTF_8)));
            HttpResponse httpResponse = client.getHttpClient().execute(httpPost);
            assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        }
    }

    private void canPut(String url, String content) throws Exception {
        try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
            HttpPut httpPut = new HttpPut(baseUrlV2 + url);
            httpPut.setEntity(
                    new StringEntity(content, ContentType.create("application/json", StandardCharsets.UTF_8)));
            HttpResponse httpResponse = client.getHttpClient().execute(httpPut);
            assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        }
    }


    private void canPut(String url, File content) throws Exception {
        try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
            HttpPut httpPut = new HttpPut(baseUrlV2 + url);
            httpPut.setEntity(new FileEntity(content));
            HttpResponse httpResponse = client.getHttpClient().execute(httpPut);
            assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        }
    }

    private void canGet(String url) throws Exception {
        canGet(url, null);
    }

    private <T> T canGet(String url, Class<T> responseType) throws Exception {
        try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
            HttpResponse httpResponse = client.getHttpClient().execute(new HttpGet(baseUrlV2 + url));
            assertEquals(200, httpResponse.getStatusLine().getStatusCode());
            if (responseType == null) {
                return null;
            }
            return objectMapper.readValue(httpResponse.getEntity().getContent(), responseType);
        }
    }

    private void canDelete(String url) throws Exception {
        try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
            HttpResponse httpResponse = client.getHttpClient().execute(new HttpDelete(baseUrlV2 + url));
            assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        }
    }

    private static class TestNodesResponseStub {
        public List<String> nodes;
    }

    private static class TestClusterResponseStub {
        public TestClusterResponseCollectionStub cluster;
    }

    private static class TestClusterResponseCollectionStub {
        public Map<String, TestClusterResponseCollectionShardsStub> collections;
    }

    private static class TestClusterResponseCollectionShardsStub {
        public Map<String, Object> shards;
    }


    // TODO Duplicated from TestConfigSetsAPI... should the configset v2 tests be there instead?
    private File createTempZipFile(String directoryPath) {
        try {
            final File zipFile = createTempFile("configset", "zip").toFile();
            final File directory = SolrTestCaseJ4.getFile(directoryPath).toFile();
            zip(directory, zipFile);
            return zipFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void zip(File directory, File zipfile) throws IOException {
        URI base = directory.toURI();
        Deque<File> queue = new ArrayDeque<>();
        queue.push(directory);
        OutputStream out = new FileOutputStream(zipfile);
        try (ZipOutputStream zout = new ZipOutputStream(out)) {
            while (!queue.isEmpty()) {
                directory = queue.pop();
                for (File kid : directory.listFiles()) {
                    String name = base.relativize(kid.toURI()).getPath();
                    if (kid.isDirectory()) {
                        queue.push(kid);
                        name = name.endsWith("/") ? name : name + "/";
                        zout.putNextEntry(new ZipEntry(name));
                    } else {
                        zout.putNextEntry(new ZipEntry(name));

                        try (InputStream in = new FileInputStream(kid)) {
                            in.transferTo(zout);
                        }

                        zout.closeEntry();
                    }
                }
            }
        }
    }
}

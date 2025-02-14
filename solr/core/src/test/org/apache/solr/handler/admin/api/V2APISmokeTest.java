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

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;

public class V2APISmokeTest extends SolrCloudTestCase {

    private URL baseUrl;
    private String baseUrlV2;

    @BeforeClass
    public static void setupCluster() throws Exception {
        configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
        baseUrlV2 = cluster.getJettySolrRunner(0).getBaseURLV2().toString();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
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

// TODO Backups API
//        canPost(collectionPath + "/backups/{backupName}/versions");

        canPost(collectionPath + "/reload", "{}");

// TODO: Shard operations
//        canPost(collectionPath + "/shards", """
//                {
//                    "name": "s1"
//                }
//                """);
//        String shardPath = collectionPath + "/shards/s1";
//        canPost(shardPath + "/force-leader");
//        canPost(shardPath + "/replicas");
//        canDelete(shardPath + "/replicas/{replicaName}");
//        canDelete(shardPath + "/replicas");
//        canPost(shardPath + "/sync");
//        canPost(shardPath + "/install");
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
        // TODO
//        canPut("/cluster/files{filePath}");
//        canDelete("/cluster/files{path}");


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
        // TODO: Node name?
//        canPost("/cluster/nodes/{nodeName}/clear");
//        canPost("/cluster/nodes/{sourceNodeName}/replace");
//        canPost("/cluster/replicas/migrate", """
//                {
//                    "sourceNodes": ["node1"]
//                }
//                """);
    }

    @Test
    public void testConfigSetsApi() throws Exception {
        canGet("/configsets");
        // TODO: need to add auth to clone _default
//        canPost("/configsets", """
//                {
//                  "name": "cfg123",
//                  "baseConfigSet": "_default"
//                }
//                """);
        // TODO need data to upload
//        canPut("/configsets/cfg123", """
//                """);
//        canPut("/configsets/cfg123/file345", """
//                """);
//        canDelete("/configsets/cfg123");
    }

    @Test
    public void testCoresApi() throws Exception {
        // TODO
//        canPost("/cores/{coreName}/backups");
//        canPost("/cores/{coreName}/install");
//        canPost("/cores/{coreName}/merge-indices");
//        canPost("/cores/{coreName}/reload");
//        canPost("/cores/{coreName}/rename");
//        canPost("/cores/{coreName}/replication/backups");
//        canGet("/cores/{coreName}/replication/files");
//        canGet("/cores/{coreName}/replication/files/{filePath}");
//        canGet("/cores/{coreName}/replication/indexversion");
//        canPost("/cores/{coreName}/restore");
//        canGet("/cores/{coreName}/segments");
//        canGet("/cores/{coreName}/snapshots");
//        canPost("/cores/{coreName}/snapshots/{snapshotName}");
//        canDelete("/cores/{coreName}/snapshots/{snapshotName}");
//        canPost("/cores/{coreName}/swap");
//        canPost("/cores/{coreName}/unload");

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
        canGet("/node/files/xyz.txt");
        canGet("/node/commands/123");
    }

    private void testCollectionsAndCoresApi(String indexType, String indexName) throws Exception {
        //indexType = collections | cores
        String indexPath = "/" + indexType + "/" + indexName;
        String schemaPath = indexPath+ "/schema"; 
        canGet(schemaPath);
        canGet(schemaPath + "/copyfields");
        canGet(schemaPath + "/dynamicfields");
//        canGet(schemaPath + "/dynamicfields/field123"); // TODO: Need a valid id
        canGet(schemaPath + "/fields");
//        canGet(schemaPath + "/fields/field123"); // TODO: Need a valid id
        canGet(schemaPath + "/fieldtypes");
//        canGet(schemaPath + "/fieldtypes/fieldType123"); // TODO: Need a valid id
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

    private void canGet(String url) throws Exception {
        try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
            HttpResponse httpResponse = client.getHttpClient().execute(new HttpGet(baseUrlV2 + url));
            assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        }
    }

    private void canDelete(String url) throws Exception {
        try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
            HttpResponse httpResponse = client.getHttpClient().execute(new HttpDelete(baseUrlV2 + url));
            assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        }
    }
}

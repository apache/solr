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
                  "numShards": 1
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


//        canPost("/collections/{collectionName}/backups/{backupName}/versions");
//
//        canPost("/collections/{collectionName}/reload");
//        canPut("/collections/{collectionName}/scale");
//
//        canPost("/collections/{collectionName}/shards");
//        canDelete("/collections/{collectionName}/shards/{shardName}");
//        canPost("/collections/{collectionName}/shards/{shardName}/force-leader");
//        canPost("/collections/{collectionName}/shards/{shardName}/replicas");
//        canDelete("/collections/{collectionName}/shards/{shardName}/replicas");
//        canDelete("/collections/{collectionName}/shards/{shardName}/replicas/{replicaName}");
//        canPost("/collections/{collectionName}/shards/{shardName}/sync");
//
//        canPost("/collections/{collName}/shards/{shardName}/install");
//        canPut("/collections/{collName}/shards/{shardName}/replicas/{replicaName}/properties/{propName}");
//        canDelete("/collections/{collName}/shards/{shardName}/replicas/{replicaName}/properties/{propName}");

        canGet(collectionPath + "/snapshots");
        //  canPost("/collections/{collName}/snapshots/{snapshotName}"); canDelete("/collections/{collName}/snapshots/{snapshotName}");

        //  canPost("/collections/{collectionName}/rename");
        testCollectionsAndCoresApi("collections", "testCollection");
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
//        canPut("/backups/{backupName}/purgeUnused", "{}");
//        canPost("/backups/{backupName}/restore", "{}");
//        canGet("/backups/{backupName}/versions");
//        canDelete("/backups/{backupName}/versions");
//        canDelete("/backups/{backupName}/versions/{backupId}");
    }

    @Test
    public void testClusterApi() throws Exception {
//        canPut("/cluster/files{filePath}");
//        canDelete("/cluster/files{path}");
//        canPost("/cluster/nodes/{nodeName}/clear");
//        canPost("/cluster/nodes/{sourceNodeName}/replace");
//        canGet("/cluster/properties");
//        canPut("/cluster/properties");
//        canGet("/cluster/properties/{propertyName}");
//        canPut("/cluster/properties/{propertyName}");
//        canDelete("/cluster/properties/{propertyName}");
//        canPost("/cluster/replicas/balance");
//        canPost("/cluster/replicas/migrate");
//        canGet("/cluster/zookeeper/children{zkPath}");
//        canGet("/cluster/zookeeper/data{zkPath}");
//        canGet("/cluster/zookeeper/data/security.json");
    }

    @Test
    public void testConfigSetsApi() throws Exception {
        canGet("/configsets");
//        canPost("/configsets", """
//                {
//                  "name": "cfg123",
//                  "baseConfigSet": "_default"
//                }
//                """);
//        canPut("/configsets/cfg123", """
//                """);
//        canPut("/configsets/cfg123/file345", """
//                """);
//        canDelete("/configsets/cfg123");
    }

    @Test
    public void testCoresApi() throws Exception {
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
        canGet("/node/logging/messages?since=" + System.currentTimeMillis());
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
//        canGet("/{indexType}/{indexName}/schema");
//        canGet("/{indexType}/{indexName}/schema/copyfields");
//        canGet("/{indexType}/{indexName}/schema/dynamicfields");
//        canGet("/{indexType}/{indexName}/schema/dynamicfields/{fieldName}");
//        canGet("/{indexType}/{indexName}/schema/fields");
//        canGet("/{indexType}/{indexName}/schema/fields/{fieldName}");
//        canGet("/{indexType}/{indexName}/schema/fieldtypes");
//        canGet("/{indexType}/{indexName}/schema/fieldtypes/{fieldTypeName}");
//        canGet("/{indexType}/{indexName}/schema/name");
//        canGet("/{indexType}/{indexName}/schema/similarity");
//        canGet("/{indexType}/{indexName}/schema/uniquekey");
//        canGet("/{indexType}/{indexName}/schema/version");
//        canGet("/{indexType}/{indexName}/schema/zkversion");
//        canGet("/{indexType}/{indexName}/select");
//        canPost("/{indexType}/{indexName}/select");
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

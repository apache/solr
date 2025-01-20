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

    /**
"/collections/{collectionName}/backups/{backupName}/versions" POST

"/collections/{collectionName}/reload" POST
"/collections/{collectionName}/scale" PUT

"/collections/{collectionName}/shards" POST
"/collections/{collectionName}/shards/{shardName}" DELETE
"/collections/{collectionName}/shards/{shardName}/force-leader" POST
"/collections/{collectionName}/shards/{shardName}/replicas" POST DELETE
"/collections/{collectionName}/shards/{shardName}/replicas/{replicaName}" DELETE
"/collections/{collectionName}/shards/{shardName}/sync" POST

"/collections/{collName}/shards/{shardName}/install" POST
"/collections/{collName}/shards/{shardName}/replicas/{replicaName}/properties/{propName}" PUT DELETE

     */
    
    canGet(collectionPath + "/snapshots");
    // "/collections/{collName}/snapshots/{snapshotName}" POST DELETE
    
    // "/collections/{collectionName}/rename" POST
    testCollectionsAndCoresApi("collections","testCollection");
    canDelete(collectionPath);
  }

  @Test
  public void testAliasesApi() throws Exception {
    /**
"/aliases" GET POST
"/aliases/{aliasName}" GET DELETE
"/aliases/{aliasName}/properties" GET PUT
"/aliases/{aliasName}/properties/{propName}" GET PUT DELETE
     */
  }

  @Test
  public void testBackupsApi() throws Exception {
    /**
"/backups/{backupName}/purgeUnused" PUT
"/backups/{backupName}/restore" POST
"/backups/{backupName}/versions" GET DELETE
"/backups/{backupName}/versions/{backupId}" DELETE

     */
  }

  @Test
  public void testClusterApi() throws Exception {
    /**
     * "/cluster/files{filePath}" PUT
     * "/cluster/files{path}" DELETE
     * "/cluster/nodes/{nodeName}/clear" POST
     * "/cluster/nodes/{sourceNodeName}/replace" POST
     * "/cluster/properties" GET PUT
     * "/cluster/properties/{propertyName}" GET PUT DELETE
     * "/cluster/replicas/balance" POST
     * "/cluster/replicas/migrate" POST
     * "/cluster/zookeeper/children{zkPath}" GET
     * "/cluster/zookeeper/data{zkPath}" GET
     * "/cluster/zookeeper/data/security.json" GET
     */
  }

  @Test
  public void testConfigSetsApi() throws Exception {
    /**
     * "/configsets" GET POST
     * "/configsets/{configSetName}" PUT DELETE
     * "/configsets/{configSetName}/{filePath}" PUT
     */
  }

  @Test
  public void testCoresApi() throws Exception {
    /**
     "/cores/{coreName}/backups" POST
     "/cores/{coreName}/install" POST
     "/cores/{coreName}/merge-indices" POST
     "/cores/{coreName}/reload" POST
     "/cores/{coreName}/rename" POST
     "/cores/{coreName}/replication/backups" POST
     "/cores/{coreName}/replication/files" GET
     "/cores/{coreName}/replication/files/{filePath}" GET
     "/cores/{coreName}/replication/indexversion" GET
     "/cores/{coreName}/restore" POST
     "/cores/{coreName}/segments" GET
     "/cores/{coreName}/snapshots" GET
     "/cores/{coreName}/snapshots/{snapshotName}" POST DELETE
     "/cores/{coreName}/swap" POST
     "/cores/{coreName}/unload" POST
     */
    testCollectionsAndCoresApi("cores", "testCore");
  }


  @Test
  public void testNodesApi() throws Exception {
    /**
     "/node/commands/{requestId}" GET
     "/node/files{path}" GET

     "/node/key" GET
     "/node/logging/levels" GET PUT
     "/node/logging/messages" GET
     "/node/logging/messages/threshold" PUT
     */
  }

  private void testCollectionsAndCoresApi(String indexType, String indexName) throws Exception {
    /*
indexType = collections | cores
  "/{indexType}/{indexName}/schema" GET
"/{indexType}/{indexName}/schema/copyfields" GET
"/{indexType}/{indexName}/schema/dynamicfields" GET
"/{indexType}/{indexName}/schema/dynamicfields/{fieldName}" GET
"/{indexType}/{indexName}/schema/fields" GET
"/{indexType}/{indexName}/schema/fields/{fieldName}" GET
"/{indexType}/{indexName}/schema/fieldtypes" GET
"/{indexType}/{indexName}/schema/fieldtypes/{fieldTypeName}" GET
"/{indexType}/{indexName}/schema/name" GET
"/{indexType}/{indexName}/schema/similarity" GET
"/{indexType}/{indexName}/schema/uniquekey" GET
"/{indexType}/{indexName}/schema/version" GET
"/{indexType}/{indexName}/schema/zkversion" GET
"/{indexType}/{indexName}/select" GET POST
   */
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

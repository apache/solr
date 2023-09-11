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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.NRT_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.PULL_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.TLOG_REPLICAS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_ID;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.hamcrest.Matchers.containsString;

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.CreateCollectionRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

/** Unit tests for {@link RestoreCollectionAPI} */
public class RestoreCollectionAPITest extends SolrTestCaseJ4 {

  @Test
  public void testReportsErrorIfBackupNameMissing() {
    final var requestBody = new RestoreCollectionAPI.RestoreCollectionRequestBody();
    requestBody.collection = "someCollection";
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new RestoreCollectionAPI(null, null, null);
              api.restoreCollection(null, requestBody);
            });

    assertEquals(400, thrown.code());
    assertEquals("Required parameter 'backupName' missing", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new RestoreCollectionAPI(null, null, null);
              api.restoreCollection("someBackupName", null);
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required request body", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    // No 'collection' set on the requestBody
    final var requestBody = new RestoreCollectionAPI.RestoreCollectionRequestBody();
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new RestoreCollectionAPI(null, null, null);
              api.restoreCollection("someBackupName", requestBody);
            });

    assertEquals(400, thrown.code());
    assertEquals("Required parameter 'collection' missing", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfProvidedCollectionNameIsInvalid() {
    final var requestBody = new RestoreCollectionAPI.RestoreCollectionRequestBody();
    requestBody.collection = "invalid$collection@name";
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new RestoreCollectionAPI(null, null, null);
              api.restoreCollection("someBackupName", requestBody);
            });

    assertEquals(400, thrown.code());
    MatcherAssert.assertThat(
        thrown.getMessage(), containsString("Invalid collection: [invalid$collection@name]"));
  }

  @Test
  public void testCreatesValidRemoteMessageForExistingCollectionRestore() {
    final var requestBody = new RestoreCollectionAPI.RestoreCollectionRequestBody();
    requestBody.collection = "someCollectionName";
    requestBody.location = "/some/location/path";
    requestBody.backupId = 123;
    requestBody.repository = "someRepositoryName";
    requestBody.async = "someAsyncId";

    final var remoteMessage =
        RestoreCollectionAPI.createRemoteMessage("someBackupName", requestBody).getProperties();

    assertEquals(7, remoteMessage.size());
    assertEquals("restore", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someCollectionName", remoteMessage.get(COLLECTION));
    assertEquals("/some/location/path", remoteMessage.get(BACKUP_LOCATION));
    assertEquals(Integer.valueOf(123), remoteMessage.get(BACKUP_ID));
    assertEquals("someRepositoryName", remoteMessage.get(BACKUP_REPOSITORY));
    assertEquals("someAsyncId", remoteMessage.get(ASYNC));
    assertEquals("someBackupName", remoteMessage.get(NAME));
  }

  @Test
  public void testCreatesValidRemoteMessageForNewCollectionRestore() {
    final var requestBody = new RestoreCollectionAPI.RestoreCollectionRequestBody();
    requestBody.collection = "someCollectionName";
    requestBody.location = "/some/location/path";
    requestBody.backupId = 123;
    requestBody.repository = "someRepositoryName";
    requestBody.async = "someAsyncId";
    final var createParams = new CreateCollectionRequestBody();
    requestBody.createCollectionParams = createParams;
    createParams.config = "someConfig";
    createParams.nrtReplicas = 123;
    createParams.tlogReplicas = 456;
    createParams.pullReplicas = 789;
    createParams.nodeSet = List.of("node1", "node2");
    createParams.properties = Map.of("foo", "bar");

    final var remoteMessage =
        RestoreCollectionAPI.createRemoteMessage("someBackupName", requestBody).getProperties();

    assertEquals(14, remoteMessage.size());
    assertEquals("restore", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someCollectionName", remoteMessage.get(COLLECTION));
    assertEquals("/some/location/path", remoteMessage.get(BACKUP_LOCATION));
    assertEquals(Integer.valueOf(123), remoteMessage.get(BACKUP_ID));
    assertEquals("someRepositoryName", remoteMessage.get(BACKUP_REPOSITORY));
    assertEquals("someAsyncId", remoteMessage.get(ASYNC));
    assertEquals("someBackupName", remoteMessage.get(NAME));
    assertEquals("someConfig", remoteMessage.get(COLL_CONF));
    assertEquals(Integer.valueOf(123), remoteMessage.get(NRT_REPLICAS));
    assertEquals(Integer.valueOf(123), remoteMessage.get(REPLICATION_FACTOR));
    assertEquals(Integer.valueOf(456), remoteMessage.get(TLOG_REPLICAS));
    assertEquals(Integer.valueOf(789), remoteMessage.get(PULL_REPLICAS));
    assertEquals("node1,node2", remoteMessage.get(CREATE_NODE_SET_PARAM));
    assertEquals("bar", remoteMessage.get("property.foo"));
  }

  @Test
  public void testCanConvertV1ParamsIntoV2RequestBody() {
    final var v1Params = new ModifiableSolrParams();
    v1Params.add("name", "someBackupName");
    v1Params.add("collection", "someCollectionName");
    v1Params.add("location", "/some/location/str");
    v1Params.add("repository", "someRepositoryName");
    v1Params.add("backupId", "123");
    v1Params.add("async", "someAsyncId");
    // Supported coll-creation params
    v1Params.add("collection.configName", "someConfig");
    v1Params.add("nrtReplicas", "123");
    v1Params.add("property.foo", "bar");
    v1Params.add("createNodeSet", "node1,node2");
    v1Params.add("createNodeSet.shuffle", "false");

    final var requestBody =
        RestoreCollectionAPI.RestoreCollectionRequestBody.fromV1Params(v1Params);

    assertEquals("someCollectionName", requestBody.collection);
    assertEquals("/some/location/str", requestBody.location);
    assertEquals("someRepositoryName", requestBody.repository);
    assertEquals(Integer.valueOf(123), requestBody.backupId);
    assertEquals("someAsyncId", requestBody.async);
    assertNotNull(requestBody.createCollectionParams);
    final var createParams = requestBody.createCollectionParams;
    assertEquals("someConfig", createParams.config);
    assertEquals(Integer.valueOf(123), createParams.nrtReplicas);
    assertNotNull(createParams.properties);
    assertEquals(1, createParams.properties.size());
    assertEquals("bar", createParams.properties.get("foo"));
    assertEquals(List.of("node1", "node2"), createParams.nodeSet);
    assertEquals(Boolean.FALSE, createParams.shuffleNodes);
  }
}

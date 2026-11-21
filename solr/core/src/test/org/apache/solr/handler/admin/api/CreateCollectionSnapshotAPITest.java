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

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionSnapshotsApi;
import org.apache.solr.client.solrj.response.JavaBinResponseParser;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.XMLResponseParser;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.eclipse.jetty.client.StringRequestContent;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/** HTTP integration tests for creating collection snapshots through the generated client. */
public class CreateCollectionSnapshotAPITest extends SolrCloudTestCase {
  private static final String COLLECTION = "snapshot_api_collection";
  private static final String ALIAS = "snapshot_api_alias";
  private static final Duration ASYNC_SNAPSHOT_TIMEOUT = Duration.ofSeconds(30);

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
    cluster.getSolrClient().add(COLLECTION, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().commit(COLLECTION);
    CollectionAdminRequest.createAlias(ALIAS, COLLECTION).process(cluster.getSolrClient());
  }

  @After
  public void clearSnapshots() throws Exception {
    var response =
        new CollectionSnapshotsApi.ListSnapshots(COLLECTION).process(cluster.getSolrClient());
    for (String snapshot : response.snapshots.keySet()) {
      new CollectionSnapshotsApi.DeleteCollectionSnapshot(COLLECTION, snapshot)
          .process(cluster.getSolrClient());
    }
  }

  @Test
  public void testMissingCollections() throws Exception {
    var client = cluster.getSolrClient();
    assertEquals(
        404,
        expectThrows(
                RemoteSolrException.class,
                () ->
                    new CollectionSnapshotsApi.CreateCollectionSnapshot("missing", "snapshot")
                        .process(client))
            .code());
    assertEquals(
        404,
        expectThrows(
                RemoteSolrException.class,
                () -> new CollectionSnapshotsApi.ListSnapshots("missing").process(client))
            .code());
    assertEquals(
        404,
        expectThrows(
                RemoteSolrException.class,
                () ->
                    new CollectionSnapshotsApi.DeleteCollectionSnapshot("missing", "snapshot")
                        .process(client))
            .code());
    assertEquals(
        400,
        expectThrows(
                RemoteSolrException.class,
                () ->
                    new CollectionAdminRequest.CreateSnapshot("missing", "snapshot")
                        .process(client))
            .code());
    assertEquals(
        400,
        expectThrows(
                RemoteSolrException.class,
                () -> new CollectionAdminRequest.ListSnapshots("missing").process(client))
            .code());
    assertEquals(
        400,
        expectThrows(
                RemoteSolrException.class,
                () ->
                    new CollectionAdminRequest.DeleteSnapshot("missing", "snapshot")
                        .process(client))
            .code());
  }

  @Test
  public void testMissingSnapshot() throws Exception {
    var request = new CollectionSnapshotsApi.DeleteCollectionSnapshot(COLLECTION, "missing");
    var error =
        expectThrows(RemoteSolrException.class, () -> request.process(cluster.getSolrClient()));
    assertEquals(404, error.code());
    assertTrue(error.getMessage(), error.getMessage().contains("missing"));
    new CollectionAdminRequest.DeleteSnapshot(COLLECTION, "missing")
        .process(cluster.getSolrClient());
  }

  @Test
  public void testMissingRequestBody() throws Exception {
    var jetty = cluster.getJettySolrRunner(0);
    var response =
        jetty
            .getSolrClient()
            .getHttpClient()
            .newRequest(
                jetty.getBaseURLV2() + "/collections/" + COLLECTION + "/snapshots/missing_body")
            .method("POST")
            .send();
    assertEquals(400, response.getStatus());
    var error = (Map<?, ?>) ((Map<?, ?>) Utils.fromJSON(response.getContent())).get("error");
    assertEquals(400L, ((Number) error.get("code")).longValue());
    assertTrue(error.toString(), error.get("msg").toString().contains("request-body"));
  }

  @Test
  public void testCreateResponseFormats() throws Exception {
    var jetty = cluster.getJettySolrRunner(0);
    for (String format : List.of("xml", "javabin")) {
      String snapshot = "snapshot_" + format;
      var response =
          jetty
              .getSolrClient()
              .getHttpClient()
              .newRequest(
                  jetty.getBaseURLV2()
                      + "/collections/"
                      + ALIAS
                      + "/snapshots/"
                      + snapshot
                      + "?wt="
                      + format)
              .method("POST")
              .body(new StringRequestContent("application/json", "{\"followAliases\":true}"))
              .send();
      assertEquals(200, response.getStatus());
      var parser = format.equals("xml") ? new XMLResponseParser() : new JavaBinResponseParser();
      var body = parser.processResponse(new ByteArrayInputStream(response.getContent()), "UTF-8");
      assertEquals(ALIAS, body.get("collection"));
      assertEquals(true, body.get("followAliases"));
      assertEquals(snapshot, body.get("snapshot"));
      var listing =
          jetty
              .getSolrClient()
              .getHttpClient()
              .newRequest(
                  jetty.getBaseURLV2() + "/collections/" + COLLECTION + "/snapshots?wt=" + format)
              .send();
      assertEquals(200, listing.getStatus());
      var listingBody =
          parser.processResponse(new ByteArrayInputStream(listing.getContent()), "UTF-8");
      assertEquals(snapshot, listingBody._get("snapshots/" + snapshot + "/name"));
      assertEquals("Successful", listingBody._get("snapshots/" + snapshot + "/status"));
    }
  }

  @Test
  public void testCreateListAndDeleteSnapshot() throws Exception {
    String snapshot = "snapshot_api_sync";
    var response =
        new CollectionSnapshotsApi.CreateCollectionSnapshot(COLLECTION, snapshot)
            .process(cluster.getSolrClient());
    assertEquals(COLLECTION, response.collection);
    assertEquals(snapshot, response.snapshotName);
    assertFalse(response.followAliases);
    var metadata =
        new CollectionSnapshotsApi.ListSnapshots(COLLECTION)
            .process(cluster.getSolrClient())
            .snapshots
            .get(snapshot);
    assertEquals(snapshot, metadata.name);
    assertEquals("Successful", metadata.status);
    assertNotNull(metadata.creationDate);
    assertEquals(List.of("shard1"), metadata.shards);
    assertEquals(1, metadata.replicaSnapshots.size());
    var replica = metadata.replicaSnapshots.getFirst();
    assertEquals("shard1", replica.shardId);
    assertTrue(replica.leader);
    assertFalse(replica.files.isEmpty());
    var legacyResponse =
        new CollectionAdminRequest.ListSnapshots(COLLECTION).process(cluster.getSolrClient());
    var legacySnapshots = (NamedList<?>) legacyResponse.getResponse().get("snapshots");
    var legacyMetadata =
        new CollectionSnapshotMetaData((NamedList<?>) legacySnapshots.get(snapshot));
    assertEquals(snapshot, legacyMetadata.getName());
    assertEquals(metadata.creationDate.longValue(), legacyMetadata.getCreationDate().getTime());
    assertEquals(replica.coreName, legacyMetadata.getReplicaSnapshots().getFirst().getCoreName());
    assertTrue(
        new CollectionSnapshotsApi.ListSnapshots(COLLECTION)
            .process(cluster.getSolrClient())
            .snapshots
            .containsKey(snapshot));

    new CollectionSnapshotsApi.DeleteCollectionSnapshot(COLLECTION, snapshot)
        .process(cluster.getSolrClient());
    assertTrue(
        new CollectionSnapshotsApi.ListSnapshots(COLLECTION)
            .process(cluster.getSolrClient())
            .snapshots
            .isEmpty());
  }

  @Test
  public void testCreateSnapshotAsync() throws Exception {
    String snapshot = "snapshot_api_async";
    String asyncId = "create_snapshot_async";
    var client = cluster.getSolrClient();
    var request = new CollectionSnapshotsApi.CreateCollectionSnapshot(ALIAS, snapshot);
    request.setFollowAliases(true);
    request.setAsync(asyncId);
    var response = request.process(client);
    assertEquals(asyncId, response.requestId);
    assertEquals(ALIAS, response.collection);
    assertEquals(snapshot, response.snapshotName);
    assertTrue(response.followAliases);

    var status = waitForAsyncClusterRequest(asyncId, ASYNC_SNAPSHOT_TIMEOUT);
    assertEquals(status.toString(), RequestStatusState.COMPLETED, status.getRequestStatus());
    var metadata =
        new CollectionSnapshotsApi.ListSnapshots(COLLECTION)
            .process(client)
            .snapshots
            .get(snapshot);
    assertNotNull(metadata);
    assertEquals(snapshot, metadata.name);
    assertEquals("Successful", metadata.status);
    assertEquals(List.of("shard1"), metadata.shards);
    assertEquals(1, metadata.replicaSnapshots.size());
    assertFalse(metadata.replicaSnapshots.getFirst().files.isEmpty());
    CollectionAdminRequest.deleteAsyncId(asyncId).process(client);
  }

  @Test
  public void testCreateSnapshotThroughAlias() throws Exception {
    String snapshot = "snapshot_api_alias";
    var request = new CollectionSnapshotsApi.CreateCollectionSnapshot(ALIAS, snapshot);
    request.setFollowAliases(true);
    var response = request.process(cluster.getSolrClient());
    assertEquals(ALIAS, response.collection);
    assertEquals(snapshot, response.snapshotName);
    assertTrue(response.followAliases);
    assertTrue(
        new CollectionSnapshotsApi.ListSnapshots(COLLECTION)
            .process(cluster.getSolrClient())
            .snapshots
            .containsKey(snapshot));

    var delete = new CollectionSnapshotsApi.DeleteCollectionSnapshot(ALIAS, snapshot);
    delete.setFollowAliases(true);
    delete.process(cluster.getSolrClient());
    assertTrue(
        new CollectionSnapshotsApi.ListSnapshots(COLLECTION)
            .process(cluster.getSolrClient())
            .snapshots
            .isEmpty());
  }
}

package org.apache.solr.handler.admin.api;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CollectionAdminParams.COPY_FILES_STRATEGY;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

/** Unit tests for {@link CreateCollectionBackupAPI} */
public class V2CollectionBackupApiTest extends SolrTestCaseJ4 {
  @Test
  public void testCreateRemoteMessageWithAllProperties() {
    final var requestBody = new CreateCollectionBackupAPI.CreateCollectionBackupRequestBody();
    requestBody.collection = "someCollectionName";
    requestBody.location = "/some/location";
    requestBody.repository = "someRepoName";
    requestBody.followAliases = true;
    requestBody.indexBackup = COPY_FILES_STRATEGY;
    requestBody.commitName = "someSnapshotName";
    requestBody.incremental = true;
    requestBody.maxNumBackupPoints = 123;
    requestBody.async = "someId";

    var message = CreateCollectionBackupAPI.createRemoteMessage("someBackupName", requestBody);
    var messageProps = message.getProperties();

    assertEquals(11, messageProps.size());
    assertEquals("someCollectionName", messageProps.get("collection"));
    assertEquals("/some/location", messageProps.get("location"));
    assertEquals("someRepoName", messageProps.get("repository"));
    assertEquals(true, messageProps.get("followAliases"));
    assertEquals("copy-files", messageProps.get("indexBackup"));
    assertEquals("someSnapshotName", messageProps.get("commitName"));
    assertEquals(true, messageProps.get("incremental"));
    assertEquals(123, messageProps.get("maxNumBackupPoints"));
    assertEquals("someId", messageProps.get("async"));
    assertEquals("backup", messageProps.get(QUEUE_OPERATION));
    assertEquals("someBackupName", messageProps.get("name"));
  }

  @Test
  public void testCreateRemoteMessageOmitsNullValues() {
    final var requestBody = new CreateCollectionBackupAPI.CreateCollectionBackupRequestBody();
    requestBody.collection = "someCollectionName";
    requestBody.location = "/some/location";

    var message = CreateCollectionBackupAPI.createRemoteMessage("someBackupName", requestBody);
    var messageProps = message.getProperties();

    assertEquals(4, messageProps.size());
    assertEquals("someCollectionName", messageProps.get("collection"));
    assertEquals("/some/location", messageProps.get("location"));
    assertEquals("backup", messageProps.get(QUEUE_OPERATION));
    assertEquals("someBackupName", messageProps.get("name"));
  }

  @Test
  public void testCanCreateV2RequestBodyFromV1Params() {
    final var params = new ModifiableSolrParams();
    params.set("collection", "someCollectionName");
    params.set("location", "/some/location");
    params.set("repository", "someRepoName");
    params.set("followAliases", "true");
    params.set("indexBackup", COPY_FILES_STRATEGY);
    params.set("commitName", "someSnapshotName");
    params.set("incremental", "true");
    params.set("maxNumBackupPoints", "123");
    params.set("async", "someId");

    final var requestBody = CreateCollectionBackupAPI.createRequestBodyFromV1Params(params);

    assertEquals("someCollectionName", requestBody.collection);
    assertEquals("/some/location", requestBody.location);
    assertEquals("someRepoName", requestBody.repository);
    assertEquals(Boolean.TRUE, requestBody.followAliases);
    assertEquals("copy-files", requestBody.indexBackup);
    assertEquals("someSnapshotName", requestBody.commitName);
    assertEquals(Boolean.TRUE, requestBody.incremental);
    assertEquals(Integer.valueOf(123), requestBody.maxNumBackupPoints);
    assertEquals("someId", requestBody.async);
  }
}

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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.client.api.model.MigrateDocsRequestBody;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for the migrate-docs request mapping, validation, and response. */
public class MigrateDocsAPITest extends MockV2APITest {

  private MigrateDocs api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
    api = new MigrateDocs(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testAllPropertiesAndAsyncId() throws Exception {
    final var body = new MigrateDocsRequestBody();
    body.target = "targetAlias";
    body.splitKey = "tenant!";
    body.forwardTimeout = 45;
    body.followAliases = true;
    body.async = "migrateAsyncId";

    final var response = api.migrateDocs("sourceAlias", body);

    validateRunCommand(
        CollectionParams.CollectionAction.MIGRATE,
        "migrateAsyncId",
        message ->
            assertEquals(
                Map.of(
                    "collection", "sourceAlias",
                    "target.collection", "targetAlias",
                    "split.key", "tenant!",
                    "forward.timeout", 45,
                    "followAliases", true),
                message));
    assertEquals("migrateAsyncId", response.requestId);
  }

  @Test
  public void testDefaults() throws Exception {
    final var response = api.migrateDocs("source", requestBody());

    validateRunCommand(
        CollectionParams.CollectionAction.MIGRATE,
        message ->
            assertEquals(
                Map.of(
                    "collection", "source",
                    "target.collection", "target",
                    "split.key", "a!",
                    "forward.timeout", 60),
                message));
    assertNull(response.requestId);
  }

  @Test
  public void testNullOptionalPropertiesAreOmitted() throws Exception {
    final var body = requestBody();
    body.forwardTimeout = null;

    api.migrateDocs("source", body);

    validateRunCommand(
        CollectionParams.CollectionAction.MIGRATE,
        message ->
            assertEquals(
                Map.of("collection", "source", "target.collection", "target", "split.key", "a!"),
                message));
  }

  @Test
  public void testExplicitFalseFollowAliases() throws Exception {
    final var body = requestBody();
    body.followAliases = false;

    api.migrateDocs("source", body);

    validateRunCommand(
        CollectionParams.CollectionAction.MIGRATE,
        message -> assertEquals(Boolean.FALSE, message.get("followAliases")));
  }

  @Test
  public void testPreservesSubResponses() throws Exception {
    final var success = Map.of("node1", Map.of("status", 0));
    final var failure = Map.of("node2", "some failure");
    final var remoteResponse = new NamedList<Object>();
    remoteResponse.add("success", success);
    remoteResponse.add("failure", failure);
    remoteResponse.add("warning", "some warning");
    when(mockCommandRunner.runCollectionCommand(any(), any(), anyLong()))
        .thenReturn(new OverseerSolrResponse(remoteResponse));

    final var response = api.migrateDocs("source", requestBody());

    assertEquals(success, response.successfulSubResponsesByNodeName);
    assertEquals(failure, response.failedSubResponsesByNodeName);
    assertEquals("some warning", response.warning);
  }

  @Test
  public void testRejectsMissingCollection() {
    final var exception =
        expectThrows(SolrException.class, () -> api.migrateDocs(null, requestBody()));
    assertEquals(400, exception.code());
    assertEquals("Missing required parameter: collection", exception.getMessage());
    verifyNoInteractions(mockCommandRunner);
  }

  @Test
  public void testRejectsMissingBody() {
    final var exception =
        expectThrows(SolrException.class, () -> api.migrateDocs("source", null));
    assertEquals(400, exception.code());
    assertEquals("Required request-body is missing", exception.getMessage());
    verifyNoInteractions(mockCommandRunner);
  }

  @Test
  public void testRejectsMissingTarget() {
    final var body = requestBody();
    body.target = null;
    final var exception =
        expectThrows(SolrException.class, () -> api.migrateDocs("source", body));
    assertEquals(400, exception.code());
    assertEquals("Missing required parameter: target", exception.getMessage());
    verifyNoInteractions(mockCommandRunner);
  }

  @Test
  public void testRejectsMissingSplitKey() {
    final var body = requestBody();
    body.splitKey = null;
    final var exception =
        expectThrows(SolrException.class, () -> api.migrateDocs("source", body));
    assertEquals(400, exception.code());
    assertEquals("Missing required parameter: splitKey", exception.getMessage());
    verifyNoInteractions(mockCommandRunner);
  }

  @Test
  public void testRejectsStandaloneMode() {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(false);
    final var exception =
        expectThrows(SolrException.class, () -> api.migrateDocs("source", requestBody()));
    assertEquals(400, exception.code());
    assertEquals("Solr instance is not running in SolrCloud mode.", exception.getMessage());
    verifyNoInteractions(mockCommandRunner);
  }

  private static MigrateDocsRequestBody requestBody() {
    final var body = new MigrateDocsRequestBody();
    body.target = "target";
    body.splitKey = "a!";
    return body;
  }
}

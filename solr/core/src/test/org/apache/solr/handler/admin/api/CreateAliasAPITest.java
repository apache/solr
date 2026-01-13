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

import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.COLLECTIONS;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.model.CategoryRoutedAliasProperties;
import org.apache.solr.client.api.model.CreateAliasRequestBody;
import org.apache.solr.client.api.model.CreateCollectionRequestBody;
import org.apache.solr.client.api.model.RoutedAliasProperties;
import org.apache.solr.client.api.model.TimeRoutedAliasProperties;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link CreateAlias} */
public class CreateAliasAPITest extends MockAPITest {

  private CreateAlias createAliasApi;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    createAliasApi = new CreateAlias(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new CreateAlias(null, null, null);
              api.createAlias(null);
            });

    assertEquals(400, thrown.code());
    assertEquals("Request body is required but missing", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfAliasNameInvalid() {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "some@invalid$alias";
    requestBody.collections = List.of("validColl1", "validColl2");

    final var thrown =
        expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));
    assertThat(thrown.getMessage(), containsString("Invalid alias"));
    assertThat(thrown.getMessage(), containsString("some@invalid$alias"));
    assertThat(
        thrown.getMessage(),
        containsString(
            "alias names must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }

  // Aliases can be normal or "routed', but not both.
  @Test
  public void testReportsErrorIfExplicitCollectionsAndRoutingParamsBothProvided() {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "validName";
    requestBody.collections = List.of("validColl1");
    final var categoryRouter = new CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(categoryRouter);

    final var thrown =
        expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));
    assertEquals(
        "Collections cannot be specified when creating a routed alias.", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfNeitherExplicitCollectionsNorRoutingParamsProvided() {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "validName";

    final var thrown =
        expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));
    assertEquals(400, thrown.code());
    assertEquals(
        "Alias creation requires either a list of either collections (for creating a traditional alias) or routers (for creating a routed alias)",
        thrown.getMessage());
  }

  @Test
  public void testRoutedAliasesMustProvideAConfigsetToUseOnCreatedCollections() {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "validName";
    final var categoryRouter = new CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(categoryRouter);
    final var createParams = new CreateCollectionRequestBody();
    createParams.numShards = 3;
    requestBody.collCreationParameters = createParams;

    final var thrown =
        expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));
    assertEquals(400, thrown.code());
    assertThat(
        thrown.getMessage(), containsString("Routed alias creation requires a configset name"));
  }

  @Test
  public void testRoutedAliasesMustNotSpecifyANameInCollectionCreationParams() {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "validName";
    final var categoryRouter = new CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(categoryRouter);
    final var createParams = new CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    // Not allowed since routed-aliases-created collections have semantically meaningful names
    // determined by the alias
    createParams.name = "someCollectionName";
    requestBody.collCreationParameters = createParams;

    final var thrown =
        expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));

    assertEquals(400, thrown.code());
    assertThat(thrown.getMessage(), containsString("cannot specify the name"));
  }

  @Test
  public void testReportsErrorIfCategoryRoutedAliasDoesntSpecifyAllRequiredParameters() {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "validName";
    final var createParams = new CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;
    final var categoryRouter = new CategoryRoutedAliasProperties();
    categoryRouter.maxCardinality = 123L;
    requestBody.routers = List.of(categoryRouter);

    final var thrown =
        expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));

    assertEquals(400, thrown.code());
    assertEquals(
        "Missing required parameter: 'field' on category routed alias", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfTimeRoutedAliasDoesntSpecifyAllRequiredParameters() {
    // No 'field' defined!
    {
      final var timeRouter = new TimeRoutedAliasProperties();
      timeRouter.start = "NOW";
      timeRouter.interval = "+5MINUTES";
      final var requestBody = requestBodyWithProvidedRouter(timeRouter);

      final var thrown =
          expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: 'field' on time routed alias", thrown.getMessage());
    }

    // No 'start' defined!
    {
      final var timeRouter = new TimeRoutedAliasProperties();
      timeRouter.field = "someField";
      timeRouter.interval = "+5MINUTES";
      final var requestBody = requestBodyWithProvidedRouter(timeRouter);

      final var thrown =
          expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: 'start' on time routed alias", thrown.getMessage());
    }

    // No 'interval' defined!
    {
      final var timeRouter = new TimeRoutedAliasProperties();
      timeRouter.field = "someField";
      timeRouter.start = "NOW";
      final var requestBody = requestBodyWithProvidedRouter(timeRouter);

      final var thrown =
          expectThrows(SolrException.class, () -> CreateAlias.validateRequestBody(requestBody));

      assertEquals(400, thrown.code());
      assertEquals(
          "Missing required parameter: 'interval' on time routed alias", thrown.getMessage());
    }
  }

  @Test
  public void testRemoteMessageCreationForTraditionalAlias() throws Exception {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "someAliasName";
    requestBody.collections = List.of("validColl1", "validColl2");
    requestBody.async = "someAsyncId";

    createAliasApi.createAlias(requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();
    assertEquals(2, remoteMessage.size());
    assertEquals("someAliasName", remoteMessage.get("name"));
    assertEquals("validColl1,validColl2", remoteMessage.get(COLLECTIONS));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.CREATEALIAS, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testRemoteMessageCreationForCategoryRoutedAlias() throws Exception {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "someAliasName";
    final var categoryRouter = new CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(categoryRouter);
    final var createParams = new CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;

    createAliasApi.createAlias(requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();
    assertEquals(5, remoteMessage.size());
    assertEquals("someAliasName", remoteMessage.get("name"));
    assertEquals("category", remoteMessage.get("router.name"));
    assertEquals("someField", remoteMessage.get("router.field"));
    assertEquals(3, remoteMessage.get("create-collection.numShards"));
    assertEquals("someConfig", remoteMessage.get("create-collection.collection.configName"));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.CREATEALIAS, context.getAction());
    assertNull(context.getAsyncId());
  }

  @Test
  public void testRemoteMessageCreationForTimeRoutedAlias() throws Exception {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "someAliasName";
    final var timeRouter = new TimeRoutedAliasProperties();
    timeRouter.field = "someField";
    timeRouter.start = "NOW/HOUR";
    timeRouter.interval = "+1MONTH";
    timeRouter.maxFutureMs = 123456L;
    requestBody.routers = List.of(timeRouter);
    final var createParams = new CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;

    createAliasApi.createAlias(requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();
    assertEquals(8, remoteMessage.size());
    assertEquals("someAliasName", remoteMessage.get("name"));
    assertEquals("time", remoteMessage.get("router.name"));
    assertEquals("someField", remoteMessage.get("router.field"));
    assertEquals("NOW/HOUR", remoteMessage.get("router.start"));
    assertEquals("+1MONTH", remoteMessage.get("router.interval"));
    assertEquals(Long.valueOf(123456L), remoteMessage.get("router.maxFutureMs"));
    assertEquals(3, remoteMessage.get("create-collection.numShards"));
    assertEquals("someConfig", remoteMessage.get("create-collection.collection.configName"));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.CREATEALIAS, context.getAction());
    assertNull(context.getAsyncId());
  }

  @Test
  public void testRemoteMessageCreationForMultiDimensionalRoutedAlias() throws Exception {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "someAliasName";
    final var timeRouter = new TimeRoutedAliasProperties();
    timeRouter.field = "someField";
    timeRouter.start = "NOW/HOUR";
    timeRouter.interval = "+1MONTH";
    timeRouter.maxFutureMs = 123456L;
    final var categoryRouter = new CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(timeRouter, categoryRouter);
    final var createParams = new CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;

    createAliasApi.createAlias(requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();
    assertEquals(10, remoteMessage.size());
    assertEquals("someAliasName", remoteMessage.get("name"));
    assertEquals("time", remoteMessage.get("router.0.name"));
    assertEquals("someField", remoteMessage.get("router.0.field"));
    assertEquals("NOW/HOUR", remoteMessage.get("router.0.start"));
    assertEquals("+1MONTH", remoteMessage.get("router.0.interval"));
    assertEquals(Long.valueOf(123456L), remoteMessage.get("router.0.maxFutureMs"));
    assertEquals("category", remoteMessage.get("router.1.name"));
    assertEquals("someField", remoteMessage.get("router.1.field"));
    assertEquals(3, remoteMessage.get("create-collection.numShards"));
    assertEquals("someConfig", remoteMessage.get("create-collection.collection.configName"));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.CREATEALIAS, context.getAction());
    assertNull(context.getAsyncId());
  }

  private CreateAliasRequestBody requestBodyWithProvidedRouter(RoutedAliasProperties router) {
    final var requestBody = new CreateAliasRequestBody();
    requestBody.name = "validName";
    final var createParams = new CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;

    requestBody.routers = List.of(router);

    return requestBody;
  }

  @Test
  public void testConvertsV1ParamsForMultiDimensionalAliasToV2RequestBody() {
    final var v1Params = new ModifiableSolrParams();
    v1Params.add("name", "someAliasName");
    v1Params.add("router.name", "Dimensional[time,category]");
    v1Params.add("router.0.field", "someField");
    v1Params.add("router.0.start", "NOW/HOUR");
    v1Params.add("router.0.interval", "+1MONTH");
    v1Params.add("router.0.maxFutureMs", "123456");
    v1Params.add("router.1.field", "someOtherField");
    v1Params.add("router.1.maxCardinality", "20");
    v1Params.add("create-collection.numShards", "3");
    v1Params.add("create-collection.collection.configName", "someConfig");

    final var requestBody = CreateAlias.createFromSolrParams(v1Params);

    assertEquals("someAliasName", requestBody.name);
    assertEquals(2, requestBody.routers.size());
    assertTrue(
        "Incorrect router type " + requestBody.routers.get(0) + " at index 0",
        requestBody.routers.get(0) instanceof TimeRoutedAliasProperties);
    final var timeRouter = (TimeRoutedAliasProperties) requestBody.routers.get(0);
    assertEquals("someField", timeRouter.field);
    assertEquals("NOW/HOUR", timeRouter.start);
    assertEquals("+1MONTH", timeRouter.interval);
    assertEquals(Long.valueOf(123456L), timeRouter.maxFutureMs);
    assertTrue(
        "Incorrect router type " + requestBody.routers.get(1) + " at index 1",
        requestBody.routers.get(1) instanceof CategoryRoutedAliasProperties);
    final var categoryRouter = (CategoryRoutedAliasProperties) requestBody.routers.get(1);
    assertEquals("someOtherField", categoryRouter.field);
    assertEquals(Long.valueOf(20), categoryRouter.maxCardinality);
    final var createCollParams = requestBody.collCreationParameters;
    assertEquals(Integer.valueOf(3), createCollParams.numShards);
    assertEquals("someConfig", createCollParams.config);
  }
  // v1 -> v2 param conversion test
}

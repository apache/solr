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
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.hamcrest.Matchers.containsString;

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

/** Unit tests for {@link CreateAliasAPI} */
public class CreateAliasAPITest extends SolrTestCaseJ4 {

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new CreateAliasAPI(null, null, null);
              api.createAlias(null);
            });

    assertEquals(400, thrown.code());
    assertEquals("Request body is required but missing", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfAliasNameInvalid() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "some@invalid$alias";
    requestBody.collections = List.of("validColl1", "validColl2");

    final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());
    MatcherAssert.assertThat(thrown.getMessage(), containsString("Invalid alias"));
    MatcherAssert.assertThat(thrown.getMessage(), containsString("some@invalid$alias"));
    MatcherAssert.assertThat(
        thrown.getMessage(),
        containsString(
            "alias names must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }

  // Aliases can be normal or "routed', but not both.
  @Test
  public void testReportsErrorIfExplicitCollectionsAndRoutingParamsBothProvided() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "validName";
    requestBody.collections = List.of("validColl1");
    final var categoryRouter = new CreateAliasAPI.CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(categoryRouter);

    final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());
    assertEquals(
        "Collections cannot be specified when creating a routed alias.", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfNeitherExplicitCollectionsNorRoutingParamsProvided() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "validName";

    final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());
    assertEquals(400, thrown.code());
    assertEquals(
        "Alias creation requires either a list of either collections (for creating a traditional alias) or routers (for creating a routed alias)",
        thrown.getMessage());
  }

  @Test
  public void testRoutedAliasesMustProvideAConfigsetToUseOnCreatedCollections() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "validName";
    final var categoryRouter = new CreateAliasAPI.CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(categoryRouter);
    final var createParams = new CreateCollectionAPI.CreateCollectionRequestBody();
    createParams.numShards = 3;
    requestBody.collCreationParameters = createParams;

    final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());
    assertEquals(400, thrown.code());
    MatcherAssert.assertThat(
        thrown.getMessage(), containsString("Routed alias creation requires a configset name"));
  }

  @Test
  public void testRoutedAliasesMustNotSpecifyANameInCollectionCreationParams() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "validName";
    final var categoryRouter = new CreateAliasAPI.CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(categoryRouter);
    final var createParams = new CreateCollectionAPI.CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    // Not allowed since routed-aliases-created collections have semantically meaningful names
    // determined by the alias
    createParams.name = "someCollectionName";
    requestBody.collCreationParameters = createParams;

    final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());

    assertEquals(400, thrown.code());
    MatcherAssert.assertThat(thrown.getMessage(), containsString("cannot specify the name"));
  }

  @Test
  public void testReportsErrorIfCategoryRoutedAliasDoesntSpecifyAllRequiredParameters() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "validName";
    final var createParams = new CreateCollectionAPI.CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;
    final var categoryRouter = new CreateAliasAPI.CategoryRoutedAliasProperties();
    categoryRouter.maxCardinality = 123L;
    requestBody.routers = List.of(categoryRouter);

    final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());

    assertEquals(400, thrown.code());
    assertEquals(
        "Missing required parameter: 'field' on category routed alias", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfTimeRoutedAliasDoesntSpecifyAllRequiredParameters() {
    // No 'field' defined!
    {
      final var timeRouter = new CreateAliasAPI.TimeRoutedAliasProperties();
      timeRouter.start = "NOW";
      timeRouter.interval = "+5MINUTES";
      final var requestBody = requestBodyWithProvidedRouter(timeRouter);

      final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: 'field' on time routed alias", thrown.getMessage());
    }

    // No 'start' defined!
    {
      final var timeRouter = new CreateAliasAPI.TimeRoutedAliasProperties();
      timeRouter.field = "someField";
      timeRouter.interval = "+5MINUTES";
      final var requestBody = requestBodyWithProvidedRouter(timeRouter);

      final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: 'start' on time routed alias", thrown.getMessage());
    }

    // No 'interval' defined!
    {
      final var timeRouter = new CreateAliasAPI.TimeRoutedAliasProperties();
      timeRouter.field = "someField";
      timeRouter.start = "NOW";
      final var requestBody = requestBodyWithProvidedRouter(timeRouter);

      final var thrown = expectThrows(SolrException.class, () -> requestBody.validate());

      assertEquals(400, thrown.code());
      assertEquals(
          "Missing required parameter: 'interval' on time routed alias", thrown.getMessage());
    }
  }

  @Test
  public void testRemoteMessageCreationForTraditionalAlias() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "someAliasName";
    requestBody.collections = List.of("validColl1", "validColl2");
    requestBody.async = "someAsyncId";

    final var remoteMessage =
        CreateAliasAPI.createRemoteMessageForTraditionalAlias(requestBody).getProperties();

    assertEquals(4, remoteMessage.size());
    assertEquals("createalias", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someAliasName", remoteMessage.get("name"));
    assertEquals("validColl1,validColl2", remoteMessage.get(COLLECTIONS));
    assertEquals("someAsyncId", remoteMessage.get(ASYNC));
  }

  @Test
  public void testRemoteMessageCreationForCategoryRoutedAlias() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "someAliasName";
    final var categoryRouter = new CreateAliasAPI.CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(categoryRouter);
    final var createParams = new CreateCollectionAPI.CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;

    final var remoteMessage =
        CreateAliasAPI.createRemoteMessageForRoutedAlias(requestBody).getProperties();

    assertEquals(6, remoteMessage.size());
    assertEquals("createalias", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someAliasName", remoteMessage.get("name"));
    assertEquals("category", remoteMessage.get("router.name"));
    assertEquals("someField", remoteMessage.get("router.field"));
    assertEquals(3, remoteMessage.get("create-collection.numShards"));
    assertEquals("someConfig", remoteMessage.get("create-collection.collection.configName"));
  }

  @Test
  public void testRemoteMessageCreationForTimeRoutedAlias() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "someAliasName";
    final var timeRouter = new CreateAliasAPI.TimeRoutedAliasProperties();
    timeRouter.field = "someField";
    timeRouter.start = "NOW";
    timeRouter.interval = "+1MONTH";
    timeRouter.maxFutureMs = 123456L;
    requestBody.routers = List.of(timeRouter);
    final var createParams = new CreateCollectionAPI.CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;

    final var remoteMessage =
        CreateAliasAPI.createRemoteMessageForRoutedAlias(requestBody).getProperties();

    assertEquals(9, remoteMessage.size());
    assertEquals("createalias", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someAliasName", remoteMessage.get("name"));
    assertEquals("time", remoteMessage.get("router.name"));
    assertEquals("someField", remoteMessage.get("router.field"));
    assertEquals("NOW", remoteMessage.get("router.start"));
    assertEquals("+1MONTH", remoteMessage.get("router.interval"));
    assertEquals(Long.valueOf(123456L), remoteMessage.get("router.maxFutureMs"));
    assertEquals(3, remoteMessage.get("create-collection.numShards"));
    assertEquals("someConfig", remoteMessage.get("create-collection.collection.configName"));
  }

  @Test
  public void testRemoteMessageCreationForMultiDimensionalRoutedAlias() {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "someAliasName";
    final var timeRouter = new CreateAliasAPI.TimeRoutedAliasProperties();
    timeRouter.field = "someField";
    timeRouter.start = "NOW";
    timeRouter.interval = "+1MONTH";
    timeRouter.maxFutureMs = 123456L;
    final var categoryRouter = new CreateAliasAPI.CategoryRoutedAliasProperties();
    categoryRouter.field = "someField";
    requestBody.routers = List.of(timeRouter, categoryRouter);
    final var createParams = new CreateCollectionAPI.CreateCollectionRequestBody();
    createParams.numShards = 3;
    createParams.config = "someConfig";
    requestBody.collCreationParameters = createParams;

    final var remoteMessage =
        CreateAliasAPI.createRemoteMessageForRoutedAlias(requestBody).getProperties();

    assertEquals(11, remoteMessage.size());
    assertEquals("createalias", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someAliasName", remoteMessage.get("name"));
    assertEquals("time", remoteMessage.get("router.0.name"));
    assertEquals("someField", remoteMessage.get("router.0.field"));
    assertEquals("NOW", remoteMessage.get("router.0.start"));
    assertEquals("+1MONTH", remoteMessage.get("router.0.interval"));
    assertEquals(Long.valueOf(123456L), remoteMessage.get("router.0.maxFutureMs"));
    assertEquals("category", remoteMessage.get("router.1.name"));
    assertEquals("someField", remoteMessage.get("router.1.field"));
    assertEquals(3, remoteMessage.get("create-collection.numShards"));
    assertEquals("someConfig", remoteMessage.get("create-collection.collection.configName"));
  }

  private CreateAliasAPI.CreateAliasRequestBody requestBodyWithProvidedRouter(
      CreateAliasAPI.RoutedAliasProperties router) {
    final var requestBody = new CreateAliasAPI.CreateAliasRequestBody();
    requestBody.name = "validName";
    final var createParams = new CreateCollectionAPI.CreateCollectionRequestBody();
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
    v1Params.add("router.0.start", "NOW");
    v1Params.add("router.0.interval", "+1MONTH");
    v1Params.add("router.0.maxFutureMs", "123456");
    v1Params.add("router.1.field", "someOtherField");
    v1Params.add("router.1.maxCardinality", "20");
    v1Params.add("create-collection.numShards", "3");
    v1Params.add("create-collection.collection.configName", "someConfig");

    final var requestBody = CreateAliasAPI.createFromSolrParams(v1Params);

    assertEquals("someAliasName", requestBody.name);
    assertEquals(2, requestBody.routers.size());
    assertTrue(
        "Incorrect router type " + requestBody.routers.get(0) + " at index 0",
        requestBody.routers.get(0) instanceof CreateAliasAPI.TimeRoutedAliasProperties);
    final var timeRouter = (CreateAliasAPI.TimeRoutedAliasProperties) requestBody.routers.get(0);
    assertEquals("someField", timeRouter.field);
    assertEquals("NOW", timeRouter.start);
    assertEquals("+1MONTH", timeRouter.interval);
    assertEquals(Long.valueOf(123456L), timeRouter.maxFutureMs);
    assertTrue(
        "Incorrect router type " + requestBody.routers.get(1) + " at index 1",
        requestBody.routers.get(1) instanceof CreateAliasAPI.CategoryRoutedAliasProperties);
    final var categoryRouter =
        (CreateAliasAPI.CategoryRoutedAliasProperties) requestBody.routers.get(1);
    assertEquals("someOtherField", categoryRouter.field);
    assertEquals(Long.valueOf(20), categoryRouter.maxCardinality);
    final var createCollParams = requestBody.collCreationParameters;
    assertEquals(Integer.valueOf(3), createCollParams.numShards);
    assertEquals("someConfig", createCollParams.config);
  }
  // v1 -> v2 param conversion test
}

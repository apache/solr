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

package org.apache.solr.handler.admin;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.admin.api.CreateCore;
import org.junit.Test;

/**
 * Unit tests for the /cores APIs.
 *
 * <p>Note that the V2 requests made by these tests are not necessarily semantically valid. They
 * shouldn't be taken as examples. In several instances, mutually exclusive JSON parameters are
 * provided. This is done to exercise conversion of all parameters, even if particular combinations
 * are never expected in the same request.
 */
public class V2CoresAPIMappingTest extends V2ApiMappingTest<CoreAdminHandler> {

  @Override
  public void populateApiBag() {
    final CoreAdminHandler handler = getRequestHandler();
  }

  @Override
  public CoreAdminHandler createUnderlyingRequestHandler() {
    return createMock(CoreAdminHandler.class);
  }

  @Override
  public boolean isCoreSpecific() {
    return false;
  }

  @Test
  public void testCreateCoreRequestBodyMappingAllParams() throws Exception {
    final ModifiableSolrParams v1Params = new ModifiableSolrParams();
    v1Params.add("action", "create");
    v1Params.add("name", "someCoreName");
    v1Params.add("instanceDir", "some/instance/dir");
    v1Params.add("config", "some-config.xml");
    v1Params.add("schema", "some-schema.xml");
    v1Params.add("dataDir", "some/data/dir");
    v1Params.add("ulogDir", "some/ulog/dir");
    v1Params.add("configSet", "someConfigset");
    v1Params.add("shard", "shard1");
    v1Params.add("loadOnStartup", "true");
    v1Params.add("transient", "true");
    v1Params.add("coreNodeName", "someNodeName");
    v1Params.add("newCollection", "true");
    v1Params.add("async", "someAsyncId");
    v1Params.add("collection", "someCollection");
    v1Params.add("property.foo", "fooVal");
    v1Params.add("property.bar", "barVal");
    v1Params.add("collection.abc", "abcVal");
    v1Params.add("collection.xyz", "xyzVal");

    final var createRequestBody = CreateCore.createRequestBodyFromV1Params(v1Params);

    assertEquals("someCoreName", createRequestBody.name);
    assertEquals("some/instance/dir", createRequestBody.instanceDir);
    assertEquals("some-config.xml", createRequestBody.config);
    assertEquals("some-schema.xml", createRequestBody.schema);
    assertEquals("some/data/dir", createRequestBody.dataDir);
    assertEquals("some/ulog/dir", createRequestBody.ulogDir);
    assertEquals("someConfigset", createRequestBody.configSet);
    assertEquals("someCollection", createRequestBody.collection);
    assertEquals("shard1", createRequestBody.shard);
    assertEquals(Boolean.TRUE, createRequestBody.loadOnStartup);
    assertEquals(Boolean.TRUE, createRequestBody.isTransient);
    assertEquals(Boolean.TRUE, createRequestBody.newCollection);
    assertEquals("someNodeName", createRequestBody.coreNodeName);
    assertNotNull(createRequestBody.properties);
    assertEquals(2, createRequestBody.properties.size());
    assertEquals("fooVal", createRequestBody.properties.get("foo"));
    assertEquals("barVal", createRequestBody.properties.get("bar"));
    assertNotNull(createRequestBody.collectionProperties);
    assertEquals(2, createRequestBody.collectionProperties.size());
    assertEquals("abcVal", createRequestBody.collectionProperties.get("abc"));
    assertEquals("xyzVal", createRequestBody.collectionProperties.get("xyz"));

    // V1 codepath handles the async/taskId differently, and it's not passed down the v2 code
    assertEquals(null, createRequestBody.async);
  }
}

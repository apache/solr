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

import static org.apache.solr.common.params.CollectionAdminParams.NUM_SHARDS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.COLLECTION;
import static org.apache.solr.common.params.CoreAdminParams.CONFIG;
import static org.apache.solr.common.params.CoreAdminParams.CONFIGSET;
import static org.apache.solr.common.params.CoreAdminParams.CORE;
import static org.apache.solr.common.params.CoreAdminParams.CORE_NODE_NAME;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.CREATE;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.STATUS;
import static org.apache.solr.common.params.CoreAdminParams.DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.INDEX_INFO;
import static org.apache.solr.common.params.CoreAdminParams.INSTANCE_DIR;
import static org.apache.solr.common.params.CoreAdminParams.LOAD_ON_STARTUP;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.NEW_COLLECTION;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA_TYPE;
import static org.apache.solr.common.params.CoreAdminParams.ROLES;
import static org.apache.solr.common.params.CoreAdminParams.SCHEMA;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;
import static org.apache.solr.common.params.CoreAdminParams.TRANSIENT;
import static org.apache.solr.common.params.CoreAdminParams.ULOG_DIR;

import java.util.Locale;
import java.util.Map;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.api.AllCoresStatusAPI;
import org.apache.solr.handler.admin.api.CreateCoreAPI;
import org.apache.solr.handler.admin.api.SingleCoreStatusAPI;
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
    apiBag.registerObject(new CreateCoreAPI(handler));
    apiBag.registerObject(new SingleCoreStatusAPI(handler));
    apiBag.registerObject(new AllCoresStatusAPI(handler));
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
  public void testCreateCoreAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cores",
            "POST",
            "{'create': {"
                + "'name': 'someCoreName', "
                + "'instanceDir': 'someInstanceDir', "
                + "'dataDir': 'someDataDir', "
                + "'ulogDir': 'someUpdateLogDirectory', "
                + "'schema': 'some-schema-file-name', "
                + "'config': 'some-config-file-name', "
                + "'configSet': 'someConfigSetName', "
                + "'loadOnStartup': true, "
                + "'isTransient': true, "
                + "'shard': 'someShardName', "
                + "'collection': 'someCollectionName', "
                + "'replicaType': 'TLOG', "
                + "'coreNodeName': 'someNodeName', "
                + "'numShards': 123, "
                + "'roles': ['role1', 'role2'], "
                + "'properties': {'prop1': 'val1', 'prop2': 'val2'}, "
                + "'newCollection': true, "
                + "'async': 'requestTrackingId' "
                + "}}");

    assertEquals(CREATE.name().toLowerCase(Locale.ROOT), v1Params.get(ACTION));
    assertEquals("someCoreName", v1Params.get(NAME));
    assertEquals("someInstanceDir", v1Params.get(INSTANCE_DIR));
    assertEquals("someDataDir", v1Params.get(DATA_DIR));
    assertEquals("someUpdateLogDirectory", v1Params.get(ULOG_DIR));
    assertEquals("some-schema-file-name", v1Params.get(SCHEMA));
    assertEquals("some-config-file-name", v1Params.get(CONFIG));
    assertEquals("someConfigSetName", v1Params.get(CONFIGSET));
    assertTrue(v1Params.getPrimitiveBool(LOAD_ON_STARTUP));
    assertTrue(v1Params.getPrimitiveBool(TRANSIENT));
    assertEquals("someShardName", v1Params.get(SHARD));
    assertEquals("someCollectionName", v1Params.get(COLLECTION));
    assertEquals("TLOG", v1Params.get(REPLICA_TYPE));
    assertEquals("someNodeName", v1Params.get(CORE_NODE_NAME));
    assertEquals(123, v1Params.getPrimitiveInt(NUM_SHARDS));
    assertEquals("role1,role2", v1Params.get(ROLES));
    assertEquals("val1", v1Params.get("property.prop1"));
    assertEquals("val2", v1Params.get("property.prop2"));
    assertTrue(v1Params.getPrimitiveBool(NEW_COLLECTION));
    assertEquals("requestTrackingId", v1Params.get(ASYNC));
  }

  @Test
  public void testSpecificCoreStatusApiAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cores/someCore", "GET", Map.of(INDEX_INFO, new String[] {"true"}));

    assertEquals(STATUS.name().toLowerCase(Locale.ROOT), v1Params.get(ACTION));
    assertEquals("someCore", v1Params.get(CORE));
    assertTrue(v1Params.getPrimitiveBool(INDEX_INFO));
  }

  @Test
  public void testAllCoreStatusApiAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params("/cores", "GET", Map.of(INDEX_INFO, new String[] {"true"}));

    assertEquals(STATUS.name().toLowerCase(Locale.ROOT), v1Params.get(ACTION));
    assertNull("Expected 'core' parameter to be null", v1Params.get(CORE));
    assertTrue(v1Params.getPrimitiveBool(INDEX_INFO));
  }
}

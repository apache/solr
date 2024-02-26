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

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CoreAdminParams.CORE;
import static org.apache.solr.common.params.CoreAdminParams.CORE_NODE_NAME;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.OTHER;
import static org.apache.solr.common.params.CoreAdminParams.RANGES;
import static org.apache.solr.common.params.CoreAdminParams.TARGET_CORE;

import java.util.Arrays;
import java.util.List;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.V2ApiMappingTest;
import org.junit.Test;

/**
 * Unit tests for the V2 APIs found in {@link org.apache.solr.handler.admin.api} that use the
 * /cores/{core} path.
 *
 * <p>Note that the V2 requests made by these tests are not necessarily semantically valid. They
 * shouldn't be taken as examples. In several instances, mutually exclusive JSON parameters are
 * provided. This is done to exercise conversion of all parameters, even if particular combinations
 * are never expected in the same request.
 */
public class V2CoreAPIMappingTest extends V2ApiMappingTest<CoreAdminHandler> {

  private static final String NO_BODY = null;

  @Override
  public CoreAdminHandler createUnderlyingRequestHandler() {
    return createMock(CoreAdminHandler.class);
  }

  @Override
  public boolean isCoreSpecific() {
    return false;
  }

  @Override
  public void populateApiBag() {
    final CoreAdminHandler handler = getRequestHandler();
    apiBag.registerObject(new RenameCoreAPI(handler));
    apiBag.registerObject(new SplitCoreAPI(handler));
    apiBag.registerObject(new RequestCoreRecoveryAPI(handler));
    apiBag.registerObject(new PrepareCoreRecoveryAPI(handler));
    apiBag.registerObject(new RequestApplyCoreUpdatesAPI(handler));
    apiBag.registerObject(new RequestSyncShardAPI(handler));
    apiBag.registerObject(new RequestBufferUpdatesAPI(handler));
  }

  @Test
  public void testRenameCoreAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cores/coreName", "POST", "{\"rename\": {\"to\": \"otherCore\"}}");

    assertEquals("rename", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(CORE));
    assertEquals("otherCore", v1Params.get(OTHER));
  }

  @Test
  public void testSplitCoreAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cores/coreName",
            "POST",
            "{"
                + "\"split\": {"
                + "\"path\": [\"path1\", \"path2\"], "
                + "\"targetCore\": [\"core1\", \"core2\"], "
                + "\"splitKey\": \"someSplitKey\", "
                + "\"getRanges\": true, "
                + "\"ranges\": \"range1,range2\", "
                + "\"async\": \"someRequestId\"}}");

    assertEquals("split", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(CORE));
    assertEquals("someSplitKey", v1Params.get(SPLIT_KEY));
    assertEquals("range1,range2", v1Params.get(RANGES));
    assertEquals("someRequestId", v1Params.get(ASYNC));
    final List<String> pathEntries = Arrays.asList(v1Params.getParams(PATH));
    assertEquals(2, pathEntries.size());
    assertTrue(pathEntries.containsAll(List.of("path1", "path2")));
    final List<String> targetCoreEntries = Arrays.asList(v1Params.getParams(TARGET_CORE));
    assertEquals(2, targetCoreEntries.size());
    assertTrue(targetCoreEntries.containsAll(List.of("core1", "core2")));
  }

  @Test
  public void testRequestCoreRecoveryAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params("/cores/coreName", "POST", "{\"request-recovery\": {}}");

    assertEquals("requestrecovery", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(CORE));
  }

  @Test
  public void testPrepareCoreRecoveryAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cores/coreName",
            "POST",
            "{\"prep-recovery\": {"
                + "\"nodeName\": \"someNodeName\", "
                + "\"coreNodeName\": \"someCoreNodeName\", "
                + "\"state\": \"someState\", "
                + "\"checkLive\": true, "
                + "\"onlyIfLeader\": true"
                + "\"onlyIfLeaderActive\": true "
                + "}}");

    assertEquals("preprecovery", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(CORE));
    assertEquals("someNodeName", v1Params.get("nodeName"));
    assertEquals("someCoreNodeName", v1Params.get(CORE_NODE_NAME));
    assertEquals("someState", v1Params.get(ZkStateReader.STATE_PROP));
    assertTrue(v1Params.getPrimitiveBool("checkLive"));
    assertTrue(v1Params.getPrimitiveBool("onlyIfLeader"));
    assertTrue(v1Params.getPrimitiveBool("onlyIfLeaderActive"));
  }

  @Test
  public void testApplyCoreUpdatesAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params("/cores/coreName", "POST", "{\"request-apply-updates\": {}}");

    assertEquals("requestapplyupdates", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(NAME));
  }

  @Test
  public void testSyncShardAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params("/cores/coreName", "POST", "{\"request-sync-shard\": {}}");

    assertEquals("requestsyncshard", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(CORE));
  }

  @Test
  public void testRequestBufferUpdatesAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params("/cores/coreName", "POST", "{\"request-buffer-updates\": {}}");

    assertEquals("requestbufferupdates", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(NAME));
  }
}

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
import static org.apache.solr.common.params.CoreAdminParams.DELETE_DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INDEX;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INSTANCE_DIR;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.OTHER;
import static org.apache.solr.common.params.CoreAdminParams.RANGES;
import static org.apache.solr.common.params.CoreAdminParams.REQUESTID;
import static org.apache.solr.common.params.CoreAdminParams.TARGET_CORE;

import java.util.Arrays;
import java.util.List;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
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
    apiBag.registerObject(new SwapCoresAPI(handler));
    apiBag.registerObject(new RenameCoreAPI(handler));
    apiBag.registerObject(new UnloadCoreAPI(handler));
    apiBag.registerObject(new MergeIndexesAPI(handler));
    apiBag.registerObject(new SplitCoreAPI(handler));
    apiBag.registerObject(new RequestCoreRecoveryAPI(handler));
    apiBag.registerObject(new PrepareCoreRecoveryAPI(handler));
    apiBag.registerObject(new RequestApplyCoreUpdatesAPI(handler));
    apiBag.registerObject(new RequestSyncShardAPI(handler));
    apiBag.registerObject(new RequestBufferUpdatesAPI(handler));
    apiBag.registerObject(new RequestCoreCommandStatusAPI(handler));
  }

  @Test
  public void testSwapCoresAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cores/coreName", "POST", "{\"swap\": {\"with\": \"otherCore\"}}");

    assertEquals("swap", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(CORE));
    assertEquals("otherCore", v1Params.get(OTHER));
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
  public void testUnloadCoreAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cores/coreName",
            "POST",
            "{"
                + "\"unload\": {"
                + "\"deleteIndex\": true, "
                + "\"deleteDataDir\": true, "
                + "\"deleteInstanceDir\": true, "
                + "\"async\": \"someRequestId\"}}");

    assertEquals("unload", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(CORE));
    assertEquals(true, v1Params.getBool(DELETE_INDEX));
    assertEquals(true, v1Params.getBool(DELETE_DATA_DIR));
    assertEquals(true, v1Params.getBool(DELETE_INSTANCE_DIR));
    assertEquals("someRequestId", v1Params.get(ASYNC));
  }

  @Test
  public void testMergeIndexesAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cores/coreName",
            "POST",
            "{"
                + "\"merge-indexes\": {"
                + "\"indexDir\": [\"dir1\", \"dir2\"], "
                + "\"srcCore\": [\"core1\", \"core2\"], "
                + "\"updateChain\": \"someUpdateChain\", "
                + "\"async\": \"someRequestId\"}}");

    assertEquals("mergeindexes", v1Params.get(ACTION));
    assertEquals("coreName", v1Params.get(CORE));
    assertEquals("someUpdateChain", v1Params.get(UpdateParams.UPDATE_CHAIN));
    assertEquals("someRequestId", v1Params.get(ASYNC));
    final List<String> indexDirs = Arrays.asList(v1Params.getParams("indexDir"));
    assertEquals(2, indexDirs.size());
    assertTrue(indexDirs.containsAll(List.of("dir1", "dir2")));
    final List<String> srcCores = Arrays.asList(v1Params.getParams("srcCore"));
    assertEquals(2, srcCores.size());
    assertTrue(srcCores.containsAll(List.of("core1", "core2")));
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

  // Strictly speaking, this API isn't at the /cores/coreName path, but as the only API at its path
  // (/cores/coreName/command-status/requestId) it doesn't merit its own test class.
  @Test
  public void testRequestCommandStatusAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params("/cores/coreName/command-status/someId", "GET", NO_BODY);

    assertEquals("requeststatus", v1Params.get(ACTION));
    assertEquals("someId", v1Params.get(REQUESTID));
  }
}

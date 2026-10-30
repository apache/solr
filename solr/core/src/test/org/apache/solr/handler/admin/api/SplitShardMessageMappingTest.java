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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.NUM_SUB_SHARDS;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_BY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_FUZZ;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_METHOD;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.TIMING;
import static org.apache.solr.common.params.CoreAdminParams.RANGES;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.client.api.model.SplitShardRequestBody;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link SplitShard} JAX-RS resource: every {@link SplitShardRequestBody} field
 * must reach the overseer message (or, for 'async', the submission's async id) under the name the
 * v1 SPLITSHARD command reads.
 *
 * <p>Mutually exclusive split options are exercised separately.
 */
public class SplitShardMessageMappingTest extends MockV2APITest {

  private SplitShard api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new SplitShard(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testSplitShardCommonProperties() throws Exception {
    final var requestBody = new SplitShardRequestBody();
    requestBody.numSubShards = 3;
    requestBody.splitFuzz = "0.1";
    requestBody.timing = true;
    requestBody.splitByPrefix = false;
    requestBody.followAliases = true;
    requestBody.splitMethod = "rewrite";
    requestBody.coreProperties = Map.of("foo", "foo1", "bar", "bar1");
    requestBody.async = "some_async_id";
    requestBody.waitForFinalState = true;

    api.splitShard("collName", "shard1", requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.SPLITSHARD,
        "some_async_id",
        message -> {
          assertEquals(11, message.size());
          assertEquals("collName", message.get(COLLECTION_PROP));
          assertEquals("shard1", message.get(SHARD_ID_PROP));
          assertEquals(3, message.get(NUM_SUB_SHARDS));
          assertEquals("0.1", message.get(SPLIT_FUZZ));
          assertEquals(Boolean.TRUE, message.get(TIMING));
          assertEquals(Boolean.FALSE, message.get(SPLIT_BY_PREFIX));
          assertEquals(Boolean.TRUE, message.get(FOLLOW_ALIASES));
          assertEquals("rewrite", message.get(SPLIT_METHOD));
          assertEquals(Boolean.TRUE, message.get(WAIT_FOR_FINAL_STATE));
          assertEquals("foo1", message.get("property.foo"));
          assertEquals("bar1", message.get("property.bar"));
        });
  }

  @Test
  public void testSplitShardWithRanges() throws Exception {
    final var requestBody = new SplitShardRequestBody();
    requestBody.ranges = "80000000-bfffffff,c0000000-7fffffff";

    api.splitShard("collName", "shard1", requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.SPLITSHARD,
        message -> {
          assertEquals(3, message.size());
          assertEquals(requestBody.ranges, message.get(RANGES));
        });
  }

  @Test
  public void testSplitShardWithSplitKey() throws Exception {
    final var requestBody = new SplitShardRequestBody();
    requestBody.splitKey = "someKey!";
    requestBody.splitByPrefix = true;

    api.splitShard("collName", "shard1", requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.SPLITSHARD,
        message -> {
          assertEquals(4, message.size());
          assertEquals(requestBody.splitKey, message.get(SPLIT_KEY));
          assertEquals(Boolean.TRUE, message.get(SPLIT_BY_PREFIX));
        });
  }

  @Test
  public void testSplitShardUsesExtendedTimeout() throws Exception {
    api.splitShard("collName", "shard1", new SplitShardRequestBody());

    verify(mockCommandRunner)
        .runCollectionCommand(any(), any(), eq(DEFAULT_COLLECTION_OP_TIMEOUT * 5));
  }

  @Test
  public void testOtherCommandsRetainDefaultTimeout() throws Exception {
    api.submitRemoteMessageAndHandleResponse(
        new SubResponseAccumulatingJerseyResponse(),
        CollectionParams.CollectionAction.CREATESHARD,
        new ZkNodeProps(Map.of(COLLECTION_PROP, "collName", SHARD_ID_PROP, "shard2")),
        null);

    verify(mockCommandRunner)
        .runCollectionCommand(any(), any(), eq(DEFAULT_COLLECTION_OP_TIMEOUT));
  }

  @Test
  public void testSplitShardPreservesTimingAndSubResponses() throws Exception {
    final var phaseTiming = new NamedList<Object>();
    phaseTiming.add("time", 7D);
    final var timing = new NamedList<Object>();
    timing.add("time", 12D);
    timing.add("split", phaseTiming);
    final var success = Map.of("node1", Map.of("status", 0));
    final var remoteResponse = new NamedList<Object>();
    remoteResponse.add(TIMING, timing);
    remoteResponse.add("success", success);
    remoteResponse.add("warning", "some warning");
    when(mockCommandRunner.runCollectionCommand(any(), any(), anyLong()))
        .thenReturn(new OverseerSolrResponse(remoteResponse));
    final var requestBody = new SplitShardRequestBody();
    requestBody.timing = true;

    final var response = api.splitShard("collName", "shard1", requestBody);

    assertNotNull(response.timing);
    assertEquals(Double.valueOf(12D), response.timing.get("time"));
    assertSame(phaseTiming, response.timing.get("split"));
    assertEquals(success, response.successfulSubResponsesByNodeName);
    assertEquals("some warning", response.warning);
  }

  @Test
  public void testSplitShardOmitsUnrequestedTiming() throws Exception {
    final var response = api.splitShard("collName", "shard1", new SplitShardRequestBody());

    assertNull(response.timing);
  }

  @Test
  public void testRejectsRangesAndSplitKey() {
    final var requestBody = new SplitShardRequestBody();
    requestBody.ranges = "80000000-bfffffff,c0000000-7fffffff";
    requestBody.splitKey = "someKey!";

    assertRejectedSplit(requestBody, "Only one of 'ranges' or 'split.key' should be specified");
  }

  @Test
  public void testRejectsNumSubShardsAndRanges() {
    final var requestBody = new SplitShardRequestBody();
    requestBody.ranges = "80000000-bfffffff,c0000000-7fffffff";
    requestBody.numSubShards = 3;

    assertRejectedSplit(
        requestBody, "numSubShards can not be specified with split.key or ranges parameters");
  }

  @Test
  public void testRejectsNumSubShardsAndSplitKey() {
    final var requestBody = new SplitShardRequestBody();
    requestBody.splitKey = "someKey!";
    requestBody.numSubShards = 3;

    assertRejectedSplit(
        requestBody, "numSubShards can not be specified with split.key or ranges parameters");
  }

  @Test
  public void testRejectsSplitFuzzAndRanges() {
    final var requestBody = new SplitShardRequestBody();
    requestBody.ranges = "80000000-bfffffff,c0000000-7fffffff";
    requestBody.splitFuzz = "0.1";

    assertRejectedSplit(
        requestBody, "fuzz can not be specified with split.key or ranges parameters");
  }

  @Test
  public void testRejectsSplitFuzzAndSplitKey() {
    final var requestBody = new SplitShardRequestBody();
    requestBody.splitKey = "someKey!";
    requestBody.splitFuzz = "0.1";

    assertRejectedSplit(
        requestBody, "fuzz can not be specified with split.key or ranges parameters");
  }

  private void assertRejectedSplit(SplitShardRequestBody requestBody, String expectedMessage) {
    final SolrException exception =
        expectThrows(
            SolrException.class, () -> api.splitShard("collName", "shard1", requestBody));

    assertEquals(400, exception.code());
    assertEquals(expectedMessage, exception.getMessage());
    verifyNoInteractions(mockCommandRunner);
  }

  @Test
  public void testSplitShardWithEmptyBody() throws Exception {
    api.splitShard("collName", "shard1", new SplitShardRequestBody());

    validateRunCommand(
        CollectionParams.CollectionAction.SPLITSHARD,
        message -> {
          assertEquals(2, message.size());
          assertEquals("collName", message.get(COLLECTION_PROP));
          assertEquals("shard1", message.get(SHARD_ID_PROP));
        });
  }

  @Test
  public void testSplitShardWithNullBody() throws Exception {
    api.splitShard("collName", "shard1", null);

    validateRunCommand(
        CollectionParams.CollectionAction.SPLITSHARD,
        message -> {
          assertEquals(2, message.size());
          assertEquals("collName", message.get(COLLECTION_PROP));
          assertEquals("shard1", message.get(SHARD_ID_PROP));
        });
  }
}

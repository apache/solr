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
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.client.api.model.SplitShardRequestBody;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link SplitShard} JAX-RS resource: every {@link SplitShardRequestBody} field
 * must reach the overseer message (or, for 'async', the submission's async id) under the name the
 * v1 SPLITSHARD command reads.
 *
 * <p>All fields are set at once, including combinations a real split would reject; the point is to
 * exercise the conversion of each field, not to model a valid request.
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
  public void testSplitShardAllProperties() throws Exception {
    final var requestBody = new SplitShardRequestBody();
    requestBody.ranges = "someRangeValues";
    requestBody.splitKey = "someSplitKey";
    requestBody.numSubShards = 123;
    requestBody.splitFuzz = "some_fuzz_value";
    requestBody.timing = true;
    requestBody.splitByPrefix = true;
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
          assertEquals(13, message.size());
          assertEquals("collName", message.get(COLLECTION_PROP));
          assertEquals("shard1", message.get(SHARD_ID_PROP));
          assertEquals("someRangeValues", message.get(RANGES));
          assertEquals("someSplitKey", message.get(SPLIT_KEY));
          assertEquals(123, message.get(NUM_SUB_SHARDS));
          assertEquals("some_fuzz_value", message.get(SPLIT_FUZZ));
          assertEquals(Boolean.TRUE, message.get(TIMING));
          assertEquals(Boolean.TRUE, message.get(SPLIT_BY_PREFIX));
          assertEquals(Boolean.TRUE, message.get(FOLLOW_ALIASES));
          assertEquals("rewrite", message.get(SPLIT_METHOD));
          assertEquals(Boolean.TRUE, message.get(WAIT_FOR_FINAL_STATE));
          assertEquals("foo1", message.get("property.foo"));
          assertEquals("bar1", message.get("property.bar"));
        });
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

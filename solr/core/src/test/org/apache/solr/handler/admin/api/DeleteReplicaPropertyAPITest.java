/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin.api;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link DeleteReplicaProperty}
 *
 * <p>End-to-end functionality is tested implicitly through v1 integration tests, so the unit tests
 * here focus primarily on how the v1 code invokes the v2 API and how the v2 API crafts its
 * overseer/Distributed State Processing RPC message.
 */
public class DeleteReplicaPropertyAPITest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testV1InvocationThrowsErrorsIfRequiredParametersMissing() {
    final var api = mock(DeleteReplicaProperty.class);
    final var allParams = new ModifiableSolrParams();
    allParams.add(COLLECTION_PROP, "someColl");
    allParams.add(SHARD_ID_PROP, "someShard");
    allParams.add(REPLICA_PROP, "someReplica");
    allParams.add(PROPERTY_PROP, "somePropName");

    {
      final var noCollectionParams = new ModifiableSolrParams(allParams);
      noCollectionParams.remove(COLLECTION_PROP);
      final SolrException e =
          expectThrows(
              SolrException.class,
              () -> {
                DeleteReplicaProperty.invokeUsingV1Inputs(api, noCollectionParams);
              });
      assertEquals("Missing required parameter: " + COLLECTION_PROP, e.getMessage());
    }

    {
      final var noShardParams = new ModifiableSolrParams(allParams);
      noShardParams.remove(SHARD_ID_PROP);
      final SolrException e =
          expectThrows(
              SolrException.class,
              () -> {
                DeleteReplicaProperty.invokeUsingV1Inputs(api, noShardParams);
              });
      assertEquals("Missing required parameter: " + SHARD_ID_PROP, e.getMessage());
    }

    {
      final var noReplicaParams = new ModifiableSolrParams(allParams);
      noReplicaParams.remove(REPLICA_PROP);
      final SolrException e =
          expectThrows(
              SolrException.class,
              () -> {
                DeleteReplicaProperty.invokeUsingV1Inputs(api, noReplicaParams);
              });
      assertEquals("Missing required parameter: " + REPLICA_PROP, e.getMessage());
    }

    {
      final var noPropertyParams = new ModifiableSolrParams(allParams);
      noPropertyParams.remove(PROPERTY_PROP);
      final SolrException e =
          expectThrows(
              SolrException.class,
              () -> {
                DeleteReplicaProperty.invokeUsingV1Inputs(api, noPropertyParams);
              });
      assertEquals("Missing required parameter: " + PROPERTY_PROP, e.getMessage());
    }
  }

  @Test
  public void testV1InvocationPassesAllProvidedParameters() throws Exception {
    final var api = mock(DeleteReplicaProperty.class);
    final var allParams = new ModifiableSolrParams();
    allParams.add(COLLECTION_PROP, "someColl");
    allParams.add(SHARD_ID_PROP, "someShard");
    allParams.add(REPLICA_PROP, "someReplica");
    allParams.add(PROPERTY_PROP, "somePropName");

    DeleteReplicaProperty.invokeUsingV1Inputs(api, allParams);

    verify(api).deleteReplicaProperty("someColl", "someShard", "someReplica", "somePropName");
  }

  @Test
  public void testV1InvocationTrimsPropertyNamePrefixIfProvided() throws Exception {
    final var api = mock(DeleteReplicaProperty.class);
    final var allParams = new ModifiableSolrParams();
    allParams.add(COLLECTION_PROP, "someColl");
    allParams.add(SHARD_ID_PROP, "someShard");
    allParams.add(REPLICA_PROP, "someReplica");
    allParams.add(PROPERTY_PROP, "property.somePropName"); // NOTE THE "property." prefix!

    DeleteReplicaProperty.invokeUsingV1Inputs(api, allParams);

    verify(api).deleteReplicaProperty("someColl", "someShard", "someReplica", "somePropName");
  }

  @Test
  public void testRPCMessageCreation() {
    final ZkNodeProps message =
        DeleteReplicaProperty.createRemoteMessage(
            "someColl", "someShard", "someReplica", "somePropName");
    final Map<String, Object> props = message.getProperties();

    assertEquals(5, props.size());
    assertEquals("deletereplicaprop", props.get(QUEUE_OPERATION));
    assertEquals("someColl", props.get(COLLECTION_PROP));
    assertEquals("someShard", props.get(SHARD_ID_PROP));
    assertEquals("someReplica", props.get(REPLICA_PROP));
    assertEquals("somePropName", props.get(PROPERTY_PROP));
  }
}

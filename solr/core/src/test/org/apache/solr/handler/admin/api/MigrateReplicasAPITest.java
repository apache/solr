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

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;
import org.apache.solr.client.api.model.MigrateReplicasRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link ReplaceNode} */
public class MigrateReplicasAPITest extends MockV2APITest {

  private MigrateReplicas api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new MigrateReplicas(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testCreatesValidOverseerMessage() throws Exception {
    MigrateReplicasRequestBody requestBody =
        new MigrateReplicasRequestBody(
            Set.of("demoSourceNode"), Set.of("demoTargetNode"), false, "async");

    api.migrateReplicas(requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.MIGRATE_REPLICAS,
        "async",
        message -> {
          assertEquals(3, message.size());
          assertEquals(Set.of("demoSourceNode"), message.get("sourceNodes"));
          assertEquals(Set.of("demoTargetNode"), message.get("targetNodes"));
          assertEquals(false, message.get("waitForFinalState"));
        });
  }

  @Test
  public void testNoTargetNodes() throws Exception {
    MigrateReplicasRequestBody requestBody =
        new MigrateReplicasRequestBody(Set.of("demoSourceNode"), null, null, null);

    api.migrateReplicas(requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.MIGRATE_REPLICAS,
        message -> {
          assertEquals(1, message.size());
          assertEquals(Set.of("demoSourceNode"), message.get("sourceNodes"));
        });
  }

  @Test
  public void testNoSourceNodesThrowsError() throws Exception {
    MigrateReplicasRequestBody requestBody1 =
        new MigrateReplicasRequestBody(
            Collections.emptySet(), Set.of("demoTargetNode"), null, null);
    assertThrows(SolrException.class, () -> api.migrateReplicas(requestBody1));
    MigrateReplicasRequestBody requestBody2 =
        new MigrateReplicasRequestBody(null, Set.of("demoTargetNode"), null, null);
    assertThrows(SolrException.class, () -> api.migrateReplicas(requestBody2));
  }
}

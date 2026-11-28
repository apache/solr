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
package org.apache.solr.cloud;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link RecoveryStrategy}. */
public class RecoveryStrategyTest extends SolrTestCase {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testRecoveryFailedPublishesTerminalStateWhenShardTermsCleanupThrows()
      throws Exception {
    CloudDescriptor cloudDescriptor = mock(CloudDescriptor.class);
    when(cloudDescriptor.getCollectionName()).thenReturn("collection1");
    when(cloudDescriptor.getShardId()).thenReturn("shard1");
    when(cloudDescriptor.getCoreNodeName()).thenReturn("core_node1");
    when(cloudDescriptor.getReplicaType()).thenReturn(Replica.Type.NRT);

    CoreDescriptor coreDescriptor = mock(CoreDescriptor.class);
    when(coreDescriptor.getName()).thenReturn("collection1_shard1_replica_n1");
    when(coreDescriptor.getCloudDescriptor()).thenReturn(cloudDescriptor);

    ZkShardTerms shardTerms = mock(ZkShardTerms.class);
    doThrow(new RuntimeException("shard terms cleanup failed"))
        .when(shardTerms)
        .recoveryFailed(anyString());

    ZkController zkController = mock(ZkController.class);
    when(zkController.getZkStateReader()).thenReturn(mock(ZkStateReader.class));
    when(zkController.getBaseUrl()).thenReturn("http://localhost:8983/solr");
    when(zkController.getShardTerms("collection1", "shard1")).thenReturn(shardTerms);

    CoreContainer coreContainer = mock(CoreContainer.class);
    when(coreContainer.getZkController()).thenReturn(zkController);

    RecoveryStrategy.RecoveryListener listener = mock(RecoveryStrategy.RecoveryListener.class);
    RecoveryStrategy strategy = new RecoveryStrategy(coreContainer, coreDescriptor, listener);

    // The cleanup failure must not propagate: the terminal state publication
    // and the listener notification still have to run.
    strategy.recoveryFailed(zkController, coreDescriptor);

    verify(zkController).publish(coreDescriptor, Replica.State.RECOVERY_FAILED);
    verify(listener).failed();
  }
}

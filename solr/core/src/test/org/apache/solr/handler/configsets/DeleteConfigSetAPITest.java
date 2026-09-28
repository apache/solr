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

package org.apache.solr.handler.configsets;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;
import org.apache.solr.SolrTestCase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link DeleteConfigSet}.
 *
 * <p>Note: This test focuses on input validation. Full deletion workflow is tested in integration
 * tests like {@code TestConfigSetsAPI} since actual deletion requires ZooKeeper interaction.
 */
public class DeleteConfigSetAPITest extends SolrTestCase {

  private CoreContainer mockCoreContainer;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void clearMocks() {
    mockCoreContainer = mock(CoreContainer.class);
  }

  @Test
  public void testNullConfigSetNameThrowsBadRequest() {
    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.deleteConfigSet(null, null));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Error message should mention missing configset name",
        ex.getMessage().contains("No configset name"));
  }

  @Test
  public void testEmptyConfigSetNameThrowsBadRequest() {
    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.deleteConfigSet("", null));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Error message should mention missing configset name",
        ex.getMessage().contains("No configset name"));
  }

  @Test
  public void testWhitespaceOnlyConfigSetNameThrowsBadRequest() {
    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.deleteConfigSet("   ", null));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Error message should mention missing configset name",
        ex.getMessage().contains("No configset name"));
  }

  @Test
  public void testTabOnlyConfigSetNameThrowsBadRequest() {
    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.deleteConfigSet("\t", null));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Error message should mention missing configset name",
        ex.getMessage().contains("No configset name"));
  }

  @Test
  public void testIfUnusedSkipsDeleteWhenConfigSetInUse() throws Exception {
    String configSetName = "myConfigSet";
    DocCollection usingCollection = mock(DocCollection.class);
    when(usingCollection.getConfigName()).thenReturn(configSetName);
    when(usingCollection.getName()).thenReturn("collectionUsingConfig");
    mockClusterStateWithCollections(Stream.of(usingCollection));

    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var response = api.deleteConfigSet(configSetName, true);

    assertFalse("Configset should not have been deleted while still in use", response.deleted);
    assertEquals(List.of("collectionUsingConfig"), response.collectionsUsingConfigSet);
  }

  @Test
  public void testIfUnusedReportsAllCollectionsStillUsingConfigSet() throws Exception {
    String configSetName = "sharedConfigSet";
    DocCollection collectionA = mock(DocCollection.class);
    when(collectionA.getConfigName()).thenReturn(configSetName);
    when(collectionA.getName()).thenReturn("collectionA");
    DocCollection collectionB = mock(DocCollection.class);
    when(collectionB.getConfigName()).thenReturn(configSetName);
    when(collectionB.getName()).thenReturn("collectionB");
    DocCollection unrelatedCollection = mock(DocCollection.class);
    when(unrelatedCollection.getConfigName()).thenReturn("someOtherConfigSet");
    mockClusterStateWithCollections(Stream.of(collectionA, unrelatedCollection, collectionB));

    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var response = api.deleteConfigSet(configSetName, true);

    assertFalse(response.deleted);
    assertEquals(List.of("collectionA", "collectionB"), response.collectionsUsingConfigSet);
  }

  private void mockClusterStateWithCollections(Stream<DocCollection> collections) {
    ClusterState mockClusterState = mock(ClusterState.class);
    when(mockClusterState.collectionStream()).thenReturn(collections);
    ZkController mockZkController = mock(ZkController.class);
    when(mockZkController.getClusterState()).thenReturn(mockClusterState);
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
    when(mockCoreContainer.getZkController()).thenReturn(mockZkController);
  }
}

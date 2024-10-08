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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentracing.noop.NoopSpan;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.GetAliasByNameResponse;
import org.apache.solr.client.api.model.ListAliasesResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZkStateReader.AliasesManager;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link ListAliases} */
public class ListAliasesAPITest extends SolrTestCaseJ4 {

  private CoreContainer mockCoreContainer;
  private ZkStateReader zkStateReader;
  private SolrQueryRequest mockQueryRequest;
  private SolrQueryResponse queryResponse;

  private ListAliases getAliasesAPI;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    AliasesManager aliasesManager = mock(AliasesManager.class);
    zkStateReader = mock(ZkStateReader.class);
    when(zkStateReader.getAliasesManager()).thenReturn(aliasesManager);

    ZkController zkController = mock(ZkController.class);
    when(zkController.getZkStateReader()).thenReturn(zkStateReader);

    mockCoreContainer = mock(CoreContainer.class);
    when(mockCoreContainer.getZkController()).thenReturn(zkController);

    mockQueryRequest = mock(SolrQueryRequest.class);
    when(mockQueryRequest.getSpan()).thenReturn(NoopSpan.INSTANCE);
    queryResponse = new SolrQueryResponse();

    getAliasesAPI = new ListAliases(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testGetAliases() throws Exception {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    Aliases aliases =
        Aliases.EMPTY
            .cloneWithCollectionAlias("alias0", "colA")
            .cloneWithCollectionAlias("alias1", "colB")
            .cloneWithCollectionAliasProperties("alias1", "pkey1", "pvalA");
    when(zkStateReader.getAliases()).thenReturn(aliases);

    ListAliasesResponse response = getAliasesAPI.getAliases();
    assertEquals(aliases.getCollectionAliasMap(), response.aliases);
    assertEquals(
        aliases.getCollectionAliasProperties("alias0"),
        response.properties.getOrDefault("alias0", Map.of()));
    assertEquals(
        aliases.getCollectionAliasProperties("alias1"),
        response.properties.getOrDefault("alias1", Map.of()));
    assertNull(response.error);
  }

  @Test
  public void testGetAliasByName() throws Exception {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    Aliases aliases =
        Aliases.EMPTY
            .cloneWithCollectionAlias("alias0", "colA")
            .cloneWithCollectionAlias("alias1", "colB")
            .cloneWithCollectionAliasProperties("alias1", "pkey1", "pvalA");
    when(zkStateReader.getAliases()).thenReturn(aliases);

    GetAliasByNameResponse response = getAliasesAPI.getAliasByName("alias1");
    assertEquals("alias1", response.alias);
    assertEquals(List.of("colB"), response.collections);
    assertEquals(Map.of("pkey1", "pvalA"), response.properties);

    assertNull(response.error);
  }
}

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
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.ListConfigsetsResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link ListConfigSets}. */
public class ListConfigSetsAPITest extends SolrTestCase {

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
  public void testSuccessfulListConfigsets() throws Exception {
    final ConfigSetService configSetService = mock(ConfigSetService.class);
    when(mockCoreContainer.getConfigSetService()).thenReturn(configSetService);
    when(configSetService.listConfigs()).thenReturn(List.of("cs1", "cs2"));

    final var response = new ListConfigSets(mockCoreContainer).listConfigSet();
    assertEquals(2, response.configSets.size());
    assertTrue(response.configSets.contains("cs1"));
    assertTrue(response.configSets.contains("cs2"));
  }

  /**
   * Test the v2 to v1 response mapping for /cluster/configs
   *
   * <p>{@link org.apache.solr.handler.admin.ConfigSetsHandler} uses {@link ListConfigSets} (and its
   * response class {@link ListConfigsetsResponse}) internally to serve the v1 version of this
   * functionality. So it's important to make sure that the v2 response stays compatible with SolrJ
   * - both because that's important in its own right and because that ensures we haven't
   * accidentally changed the v1 response format.
   */
  @Test
  public void testListConfigsetsV1Compatibility() throws Exception {
    final ConfigSetService configSetService = mock(ConfigSetService.class);
    when(mockCoreContainer.getConfigSetService()).thenReturn(configSetService);
    when(configSetService.listConfigs()).thenReturn(List.of("cs1", "cs2"));

    final var response = new ListConfigSets(mockCoreContainer).listConfigSet();
    final NamedList<Object> squashedResponse = new NamedList<>();
    V2ApiUtils.squashIntoNamedList(squashedResponse, response);
    final ConfigSetAdminResponse.List solrjResponse = new ConfigSetAdminResponse.List();
    solrjResponse.setResponse(squashedResponse);

    final List<String> configsets = solrjResponse.getConfigSets();
    assertEquals(2, configsets.size());
    assertTrue(configsets.contains("cs1"));
    assertTrue(configsets.contains("cs2"));
  }
}

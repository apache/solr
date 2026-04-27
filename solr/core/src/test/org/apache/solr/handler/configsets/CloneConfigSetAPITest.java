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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.CloneConfigsetRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link CloneConfigSet#cloneExistingConfigSet(CloneConfigsetRequestBody)}. */
public class CloneConfigSetAPITest extends SolrTestCase {

  private CoreContainer mockCoreContainer;
  private ConfigSetService mockConfigSetService;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setup() {
    mockCoreContainer = mock(CoreContainer.class);
    mockConfigSetService = mock(ConfigSetService.class);
    when(mockCoreContainer.getConfigSetService()).thenReturn(mockConfigSetService);
  }

  @Test
  public void testMissingBaseConfigSetDefaultsToDefaultConfigset() throws Exception {
    when(mockConfigSetService.checkConfigExists("newconfig")).thenReturn(false);
    when(mockConfigSetService.checkConfigExists(CloneConfigsetRequestBody.DEFAULT_CONFIGSET))
        .thenReturn(false);

    final var requestBody = new CloneConfigsetRequestBody();
    requestBody.name = "newconfig";
    requestBody.baseConfigSet = null;

    final var api = new CloneConfigSet(mockCoreContainer, null, null);
    final SolrException ex =
        assertThrows(SolrException.class, () -> api.cloneExistingConfigSet(requestBody));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        ex.getMessage()
            .contains(
                "Base ConfigSet does not exist: " + CloneConfigsetRequestBody.DEFAULT_CONFIGSET));
    verify(mockConfigSetService).checkConfigExists(CloneConfigsetRequestBody.DEFAULT_CONFIGSET);
  }
}

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

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
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
    final var ex = assertThrows(SolrException.class, () -> api.deleteConfigSet(null));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Error message should mention missing configset name",
        ex.getMessage().contains("No configset name"));
  }

  @Test
  public void testEmptyConfigSetNameThrowsBadRequest() {
    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.deleteConfigSet(""));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Error message should mention missing configset name",
        ex.getMessage().contains("No configset name"));
  }

  @Test
  public void testWhitespaceOnlyConfigSetNameThrowsBadRequest() {
    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.deleteConfigSet("   "));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Error message should mention missing configset name",
        ex.getMessage().contains("No configset name"));
  }

  @Test
  public void testTabOnlyConfigSetNameThrowsBadRequest() {
    final var api = new DeleteConfigSet(mockCoreContainer, null, null);
    final var ex = assertThrows(SolrException.class, () -> api.deleteConfigSet("\t"));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Error message should mention missing configset name",
        ex.getMessage().contains("No configset name"));
  }
}

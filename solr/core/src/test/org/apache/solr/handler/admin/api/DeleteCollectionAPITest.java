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

import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionParams.NAME;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.Mockito.when;

import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** Unit tests for {@link DeleteCollection} */
public class DeleteCollectionAPITest extends MockV2APITest {

  private DeleteCollection api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new DeleteCollection(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testConstructsValidOverseerMessage() throws Exception {
    // Only required properties provided
    api.deleteCollection("someCollName", null, null);

    validateRunCommand(
        CollectionParams.CollectionAction.DELETE,
        message -> {
          assertEquals(1, message.size());
          assertThat(message, hasEntry(NAME, "someCollName"));
        });

    // Optional property 'followAliases' also provided
    Mockito.clearInvocations(mockCommandRunner);
    api.deleteCollection("someCollName", true, "test");

    validateRunCommand(
        CollectionParams.CollectionAction.DELETE,
        "test",
        message -> {
          assertEquals(2, message.size());
          assertThat(message, hasEntry(NAME, "someCollName"));
          assertThat(message, hasEntry(FOLLOW_ALIASES, Boolean.TRUE));
        });
  }
}

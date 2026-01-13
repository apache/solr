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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** Unit tests for {@link DeleteCollection} */
public class DeleteCollectionAPITest extends MockAPITest {

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
    {
      api.deleteCollection("someCollName", null, null);
      verify(mockCommandRunner)
          .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

      final ZkNodeProps message = messageCapturer.getValue();
      final Map<String, Object> rawMessage = message.getProperties();
      assertEquals(1, rawMessage.size());
      assertThat(rawMessage, hasEntry(NAME, "someCollName"));

      AdminCmdContext context = contextCapturer.getValue();
      assertEquals(CollectionParams.CollectionAction.DELETE, context.getAction());
      assertNull(context.getAsyncId());
    }

    // Optional property 'followAliases' also provided
    {
      Mockito.clearInvocations(mockCommandRunner);
      api.deleteCollection("someCollName", true, "test");
      verify(mockCommandRunner)
          .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

      final ZkNodeProps message = messageCapturer.getValue();
      final Map<String, Object> rawMessage = message.getProperties();
      assertEquals(2, rawMessage.size());
      assertThat(rawMessage, hasEntry(NAME, "someCollName"));
      assertThat(rawMessage, hasEntry(FOLLOW_ALIASES, Boolean.TRUE));

      AdminCmdContext context = contextCapturer.getValue();
      assertEquals(CollectionParams.CollectionAction.DELETE, context.getAction());
      assertEquals("test", context.getAsyncId());
    }
  }
}

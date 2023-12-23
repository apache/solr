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

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionParams.NAME;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

/** Unit tests for {@link DeleteCollection} */
public class DeleteCollectionAPITest extends SolrTestCaseJ4 {

  @Test
  public void testConstructsValidOverseerMessage() {
    // Only required properties provided
    {
      final ZkNodeProps message = DeleteCollection.createRemoteMessage("someCollName", null, null);
      final Map<String, Object> rawMessage = message.getProperties();
      assertEquals(2, rawMessage.size());
      MatcherAssert.assertThat(rawMessage.keySet(), containsInAnyOrder(QUEUE_OPERATION, NAME));
      assertEquals("delete", rawMessage.get(QUEUE_OPERATION));
      assertEquals("someCollName", rawMessage.get(NAME));
    }

    // Optional properties ('followAliases' and 'async') also provided
    {
      final ZkNodeProps message =
          DeleteCollection.createRemoteMessage("someCollName", Boolean.TRUE, "someAsyncId");
      final Map<String, Object> rawMessage = message.getProperties();
      assertEquals(4, rawMessage.size());
      MatcherAssert.assertThat(
          rawMessage.keySet(), containsInAnyOrder(QUEUE_OPERATION, NAME, ASYNC, FOLLOW_ALIASES));
      assertEquals("delete", rawMessage.get(QUEUE_OPERATION));
      assertEquals("someCollName", rawMessage.get(NAME));
      assertEquals(Boolean.TRUE, rawMessage.get(FOLLOW_ALIASES));
      assertEquals("someAsyncId", rawMessage.get(ASYNC));
    }
  }
}

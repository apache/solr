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
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CoreAdminParams;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class CreateCollectionSnapshotAPITest extends SolrTestCaseJ4 {

  @Test
  public void testConstructsValidOverseerMessage() {
    final ZkNodeProps messageOne =
        CreateCollectionSnapshotAPI.createRemoteMessage(
            "myCollName", false, "mySnapshotName", null);
    final Map<String, Object> rawMessageOne = messageOne.getProperties();
    assertEquals(4, rawMessageOne.size());
    MatcherAssert.assertThat(
        rawMessageOne.keySet(),
        containsInAnyOrder(
            QUEUE_OPERATION, COLLECTION_PROP, CoreAdminParams.COMMIT_NAME, FOLLOW_ALIASES));
    assertEquals("createsnapshot", rawMessageOne.get(QUEUE_OPERATION));
    assertEquals("myCollName", rawMessageOne.get(COLLECTION_PROP));
    assertEquals("mySnapshotName", rawMessageOne.get(CoreAdminParams.COMMIT_NAME));
    assertEquals(false, rawMessageOne.get(FOLLOW_ALIASES));

    final ZkNodeProps messageTwo =
        CreateCollectionSnapshotAPI.createRemoteMessage(
            "myCollName", true, "mySnapshotName", "myAsyncId");
    final Map<String, Object> rawMessageTwo = messageTwo.getProperties();
    assertEquals(5, rawMessageTwo.size());
    MatcherAssert.assertThat(
        rawMessageTwo.keySet(),
        containsInAnyOrder(
            QUEUE_OPERATION, COLLECTION_PROP, CoreAdminParams.COMMIT_NAME, FOLLOW_ALIASES, ASYNC));
    assertEquals("createsnapshot", rawMessageTwo.get(QUEUE_OPERATION));
    assertEquals("myCollName", rawMessageTwo.get(COLLECTION_PROP));
    assertEquals("mySnapshotName", rawMessageTwo.get(CoreAdminParams.COMMIT_NAME));
    assertEquals(true, rawMessageTwo.get(FOLLOW_ALIASES));
    assertEquals("myAsyncId", rawMessageTwo.get(ASYNC));
  }
}

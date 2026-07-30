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

package org.apache.solr.handler.admin.api;

import static org.mockito.Mockito.when;

import org.apache.solr.client.api.model.AddReplicaPropertyRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link AddReplicaProperty} */
public class AddReplicaPropertyAPITest extends MockV2APITest {

  private static final AddReplicaPropertyRequestBody ANY_REQ_BODY =
      new AddReplicaPropertyRequestBody("anyValue");

  private AddReplicaProperty api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new AddReplicaProperty(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorWhenCalledInStandaloneMode() {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(false);

    final SolrException e =
        expectThrows(
            SolrException.class,
            () -> {
              api.addReplicaProperty(
                  "someColl", "someShard", "someReplica", "somePropName", ANY_REQ_BODY);
            });
    assertEquals(400, e.code());
    assertTrue(
        "Exception message differed from expected: " + e.getMessage(),
        e.getMessage().contains("not running in SolrCloud mode"));
  }

  @Test
  public void testCreatesValidOverseerMessage() throws Exception {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api.addReplicaProperty("someColl", "someShard", "someReplica", "somePropName", ANY_REQ_BODY);

    validateRunCommand(
        CollectionParams.CollectionAction.ADDREPLICAPROP,
        message -> {
          assertEquals(5, message.size());
          assertEquals("someColl", message.get("collection"));
          assertEquals("someShard", message.get("shard"));
          assertEquals("someReplica", message.get("replica"));
          assertEquals("somePropName", message.get("property"));
          assertEquals("anyValue", message.get("property.value"));
        });
  }
}

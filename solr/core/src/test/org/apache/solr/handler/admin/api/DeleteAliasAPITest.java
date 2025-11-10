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
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.NAME;

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/** Unit tests for {@link DeleteAlias}. */
public class DeleteAliasAPITest extends SolrTestCaseJ4 {

  @Test
  public void testConstructsValidRemoteMessage() {
    Map<String, Object> props = DeleteAlias.createRemoteMessage("aliasName", null).getProperties();
    assertEquals(2, props.size());
    assertEquals("deletealias", props.get(QUEUE_OPERATION));
    assertEquals("aliasName", props.get(NAME));

    props = DeleteAlias.createRemoteMessage("aliasName", "asyncId").getProperties();
    assertEquals(3, props.size());
    assertEquals("deletealias", props.get(QUEUE_OPERATION));
    assertEquals("aliasName", props.get(NAME));
    assertEquals("asyncId", props.get(ASYNC));
  }
}

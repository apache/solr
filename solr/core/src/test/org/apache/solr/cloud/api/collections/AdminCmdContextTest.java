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

package org.apache.solr.cloud.api.collections;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.common.params.CollectionAdminParams.CALLING_LOCK_ID_HEADER;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link AdminCmdContext}. */
public class AdminCmdContextTest extends SolrTestCase {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void toleratesNullRequest() {
    // req is null when this constructor runs for a V2 request that failed before V2HttpCall
    // attached a SolrQueryRequest to the Jersey request context (see SOLR-18324); this must not
    // throw, and callingLockId simply stays unset.
    AdminCmdContext ctx = new AdminCmdContext(ADDREPLICA, "async123", null);

    assertEquals(ADDREPLICA, ctx.getAction());
    assertEquals("async123", ctx.getAsyncId());
    assertNull(ctx.getCallingLockId());
  }

  @Test
  public void readsCallingLockIdFromRequestContextWhenPresent() {
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    when(req.getContext()).thenReturn(Map.of(CALLING_LOCK_ID_HEADER, "lock-42"));

    AdminCmdContext ctx = new AdminCmdContext(ADDREPLICA, "async123", req);

    assertEquals("lock-42", ctx.getCallingLockId());
  }
}

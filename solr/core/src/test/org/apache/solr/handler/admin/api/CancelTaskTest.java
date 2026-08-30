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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.CancelTaskResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CancellableQueryTracker;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CancellableCollector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CancelTaskTest extends SolrTestCaseJ4 {

  private SolrQueryRequest mockQueryRequest;
  private SolrCore solrCore;
  private CancellableQueryTracker cancellableQueryTracker;

  private CancelTask cancelTask;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    mockQueryRequest = mock(SolrQueryRequest.class);
    solrCore = mock(SolrCore.class);
    cancellableQueryTracker = mock(CancellableQueryTracker.class);

    when(mockQueryRequest.getCore()).thenReturn(solrCore);
    when(solrCore.getCancellableQueryTracker()).thenReturn(cancellableQueryTracker);

    cancelTask = new CancelTask(mockQueryRequest);
  }

  @Test
  public void testCancelRunningTask() throws Exception {
    CancellableCollector cancellableCollector = mock(CancellableCollector.class);
    when(cancellableQueryTracker.getCancellableTask("taskID_running"))
        .thenReturn(cancellableCollector);

    CancelTaskResponse response = cancelTask.cancelRunningTask("taskID_running");

    assertEquals(CancelTaskResponse.CancellationStatus.SUCCESS, response.status);
    assertNull(response.error);
    verify(cancellableCollector).cancel();
  }

  /**
   * Cancelling a task that doesn't exist should produce a real HTTP 404, the same way any other
   * v2 API request for a specific resource that isn't found does (see the "Errors" section of
   * dev-docs/v2-api-conventions.adoc). That means throwing a {@link SolrException} with {@link
   * SolrException.ErrorCode#NOT_FOUND} -- not returning a normal (200) response with a "not
   * found" value stuffed inside it, which is what {@code CancelTask.cancelRunningTask} currently
   * does. A normal Jersey return always produces a 200 on the wire regardless of what's inside
   * the response body; only a thrown exception (via CatchAllExceptionMapper) actually changes the
   * HTTP status code. This test intentionally fails against the current implementation.
   */
  @Test
  public void testCancelNonExistentTaskReturns404() {
    when(cancellableQueryTracker.getCancellableTask("taskID_missing")).thenReturn(null);

    SolrException ex =
        expectThrows(SolrException.class, () -> cancelTask.cancelRunningTask("taskID_missing"));
    assertEquals(SolrException.ErrorCode.NOT_FOUND.code, ex.code());
  }
}

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
import static org.mockito.Mockito.when;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.TaskStatusResponse;
import org.apache.solr.core.CancellableQueryTracker;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetTaskStatusTest extends SolrTestCaseJ4 {

  private SolrQueryRequest mockQueryRequest;
  private SolrCore solrCore;
  private CancellableQueryTracker cancellableQueryTracker;

  private GetTaskStatus getTaskStatus;

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

    getTaskStatus = new GetTaskStatus(mockQueryRequest);
  }

  @Test
  public void testGetTaskStatus() throws Exception {

    when(mockQueryRequest.getCore()).thenReturn(solrCore);
    when(solrCore.getCancellableQueryTracker()).thenReturn(cancellableQueryTracker);
    when(cancellableQueryTracker.isQueryIdActive("taskID_running")).thenReturn(true);
    when(cancellableQueryTracker.isQueryIdActive("taskID_stopped")).thenReturn(false);

    TaskStatusResponse taskStatusResponse;

    taskStatusResponse = getTaskStatus.getTaskStatus("taskID_running");
    assertEquals(TaskStatusResponse.TaskStatus.ACTIVE, taskStatusResponse.taskStatus);
    assertNull(taskStatusResponse.error);

    taskStatusResponse = getTaskStatus.getTaskStatus("taskID_stopped");
    assertEquals(TaskStatusResponse.TaskStatus.INACTIVE, taskStatusResponse.taskStatus);
    assertNull(taskStatusResponse.error);
  }
}

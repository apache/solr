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

import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.endpoint.GetNodeCommandStatusApi;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.CoreAdminHandler.CoreAdminAsyncTracker.TaskObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for {@link GetNodeCommandStatus}. */
public class GetNodeCommandStatusTest extends SolrTestCase {
  private CoreContainer mockCoreContainer;
  private CoreAdminHandler.CoreAdminAsyncTracker mockAsyncTracker;
  private GetNodeCommandStatusApi requestNodeCommandApi;

  @BeforeClass
  public static void ensureWorkingMockito() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Before
  public void setupMocks() {
    mockCoreContainer = mock(CoreContainer.class);
    mockAsyncTracker = mock(CoreAdminHandler.CoreAdminAsyncTracker.class);
    requestNodeCommandApi =
        new GetNodeCommandStatus(mockCoreContainer, mockAsyncTracker, null, null);
  }

  public void resetMocks() {}

  @Test
  public void testNonexistentCommandId() {
    final var response = requestNodeCommandApi.getCommandStatus("NOTFOUND-1");
    assertEquals("notfound", response.status);
  }

  @Test
  public void testReturnsStatusOfRunningCommandId() {
    final var runningTaskId = "RUNNING-1";
    whenTaskExistsWithStatus(runningTaskId, CoreAdminHandler.CoreAdminAsyncTracker.RUNNING);

    final var response = requestNodeCommandApi.getCommandStatus(runningTaskId);

    assertEquals("running", response.status);
  }

  @Test
  public void testReturnsStatusOfCompletedCommandId() {
    final var completedTaskId = "COMPLETED-1";
    whenTaskExistsWithStatus(completedTaskId, CoreAdminHandler.CoreAdminAsyncTracker.COMPLETED);

    final var response = requestNodeCommandApi.getCommandStatus(completedTaskId);

    assertEquals("completed", response.status);
  }

  @Test
  public void testReturnsStatusOfFailedCommandId() {
    final var willFailTaskId = "failed-1";
    whenTaskExistsWithStatus(willFailTaskId, CoreAdminHandler.CoreAdminAsyncTracker.FAILED);

    final var response = requestNodeCommandApi.getCommandStatus(willFailTaskId);

    assertEquals("failed", response.status);
  }

  private void whenTaskExistsWithStatus(String taskId, String status) {
    TaskObject taskObject = mock(TaskObject.class);
    when(taskObject.getStatus()).thenReturn(status);

    when(mockAsyncTracker.getAsyncRequestForStatus(taskId)).thenReturn(taskObject);
  }
}

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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.client.api.model.TaskStatusResponse;
import org.apache.solr.core.CancellableQueryTracker;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ListActiveTasksTest extends SolrTestCaseJ4 {

  private SolrQueryRequest mockQueryRequest;
  private SolrCore solrCore;
  private CancellableQueryTracker cancellableQueryTracker;

  private ListActiveTasks listActiveTasks;

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

    listActiveTasks = new ListActiveTasks(mockQueryRequest);
  }

  @Test
  public void testGetActiveTasks() throws Exception {

    Map<String, String> myMap = new LinkedHashMap<>();
    myMap.put("taskID1", "/search?q=h&gf=text-1");
    myMap.put("taskID2", "/search?q=h&gf=text-2");
    Iterator<Map.Entry<String, String>> mockIterator = myMap.entrySet().iterator();

    when(mockQueryRequest.getCore()).thenReturn(solrCore);
    when(solrCore.getCancellableQueryTracker()).thenReturn(cancellableQueryTracker);
    when(cancellableQueryTracker.getActiveQueriesGenerated()).thenReturn(mockIterator);

    ListActiveTaskResponse response = listActiveTasks.listAllActiveTasks();
    assertNotNull(response.taskList);

    assertEquals(2, response.taskList.size());

    assertEquals("taskID1", response.taskList.get(0).taskUUID);
    assertEquals("/search?q=h&gf=text-1", response.taskList.get(0).taskQuery);

    assertNull(response.error);
  }

  @Test
  public void testGetTaskStatus() throws Exception {

    when(mockQueryRequest.getCore()).thenReturn(solrCore);
    when(solrCore.getCancellableQueryTracker()).thenReturn(cancellableQueryTracker);
    when(cancellableQueryTracker.isQueryIdActive("taskID_running")).thenReturn(true);
    when(cancellableQueryTracker.isQueryIdActive("taskID_stopped")).thenReturn(false);

    TaskStatusResponse responseRunningTask = listActiveTasks.getTaskStatus("taskID_running");
    assertTrue(responseRunningTask.taskStatus);
    assertNull(responseRunningTask.error);

    TaskStatusResponse responseStoppedTask = listActiveTasks.getTaskStatus("taskID_stopped");
    assertFalse(responseStoppedTask.taskStatus);
    assertNull(responseStoppedTask.error);
  }
}

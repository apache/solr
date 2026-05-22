package org.apache.solr.handler.admin.api;

import io.opentelemetry.api.trace.Span;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.ActiveTaskDetails;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.client.api.model.ListAliasesResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListActiveTasksTest extends SolrTestCaseJ4 {

  private SolrQueryRequest mockQueryRequest;

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

    listActiveTasks = new ListActiveTasks(mockQueryRequest);
  }

  @Test
  public void testGetActiveTasks() throws Exception {

    Map<String, String> myMap = new HashMap<>();
    myMap.put("Key1", "Value1");
    myMap.put("Key2", "Value2");
    Iterator<Map.Entry<String, String>> mockIterator = myMap.entrySet().iterator();

    when(mockQueryRequest.getCore().getCancellableQueryTracker().getActiveQueriesGenerated()).thenReturn(mockIterator);

    ListActiveTaskResponse response = listActiveTasks.listAllActiveTasks();

  }

}

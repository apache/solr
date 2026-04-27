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
package org.apache.solr.handler.component;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.SolrIndexSearcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class QueryComponentFieldExistsQueryErrorHandlingTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() {
    assumeWorkingMockito();
  }

  @Test
  public void mapsFieldExistsQueryIllegalStateExceptionToBadRequest() throws Exception {
    SolrIndexSearcher searcher = Mockito.mock(SolrIndexSearcher.class);
    IllegalStateException fieldExistsError =
        new IllegalStateException("FieldExistsQuery requires norms, docValues, or vectors");
    Mockito.when(searcher.search(Mockito.any(QueryCommand.class))).thenThrow(fieldExistsError);

    InvocationTargetException thrown =
        expectThrows(
            InvocationTargetException.class,
            () -> invokeDoProcessUngroupedSearch(newResponseBuilder(searcher), new QueryCommand()));

    assertTrue(thrown.getCause() instanceof SolrException);
    SolrException solrException = (SolrException) thrown.getCause();
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, solrException.code());
    assertTrue(solrException.getMessage().startsWith("FieldExistsQuery requires"));
  }

  @Test
  public void mapsNestedFieldExistsQueryIllegalStateExceptionToBadRequest() throws Exception {
    SolrIndexSearcher searcher = Mockito.mock(SolrIndexSearcher.class);
    IllegalStateException rootCause =
        new IllegalStateException("FieldExistsQuery requires norms, docValues, or vectors");
    IllegalStateException wrapped = new IllegalStateException("outer", rootCause);
    Mockito.when(searcher.search(Mockito.any(QueryCommand.class))).thenThrow(wrapped);

    InvocationTargetException thrown =
        expectThrows(
            InvocationTargetException.class,
            () -> invokeDoProcessUngroupedSearch(newResponseBuilder(searcher), new QueryCommand()));

    assertTrue(thrown.getCause() instanceof SolrException);
    SolrException solrException = (SolrException) thrown.getCause();
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, solrException.code());
    assertTrue(solrException.getMessage().startsWith("FieldExistsQuery requires"));
  }

  @Test
  public void leavesOtherIllegalStateExceptionsUnchanged() throws Exception {
    SolrIndexSearcher searcher = Mockito.mock(SolrIndexSearcher.class);
    IllegalStateException nonFieldExistsError =
        new IllegalStateException("not a field exists query");
    Mockito.when(searcher.search(Mockito.any(QueryCommand.class))).thenThrow(nonFieldExistsError);

    InvocationTargetException thrown =
        expectThrows(
            InvocationTargetException.class,
            () -> invokeDoProcessUngroupedSearch(newResponseBuilder(searcher), new QueryCommand()));

    assertSame(nonFieldExistsError, thrown.getCause());
  }

  private static void invokeDoProcessUngroupedSearch(ResponseBuilder rb, QueryCommand cmd)
      throws Exception {
    Method method =
        QueryComponent.class.getDeclaredMethod(
            "doProcessUngroupedSearch", ResponseBuilder.class, QueryCommand.class);
    method.setAccessible(true);
    method.invoke(new QueryComponent(), rb, cmd);
  }

  private static ResponseBuilder newResponseBuilder(SolrIndexSearcher searcher) {
    SolrQueryRequest req = Mockito.mock(SolrQueryRequest.class);
    SolrQueryResponse rsp = Mockito.mock(SolrQueryResponse.class);
    Mockito.when(req.getSearcher()).thenReturn(searcher);
    return new ResponseBuilder(req, rsp, new ArrayList<>(0));
  }
}

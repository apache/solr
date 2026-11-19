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

import java.util.ArrayList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.SolrReturnFields;
import org.junit.Test;
import org.mockito.Mockito;

public class QueryComponentReturnFieldsNpeTest extends SolrTestCaseJ4 {

  // LBSolrClient's cause-less exception message
  private static final String EXCEPTION_MESSAGE = "Time allowed to handle this request exceeded";

  private static final String EXPECTED_ERROR =
      "org.apache.solr.client.solrj.SolrServerException: " + EXCEPTION_MESSAGE;

  // SOLR-18596: a cause-less SolrServerException must be recorded, not cause an NPE.
  @Test
  public void recordsCauselessSolrServerException() {
    assumeWorkingMockito();
    SimpleOrderedMap<Object> shardEntry = new SimpleOrderedMap<>();
    ResponseBuilder rb = getFieldsResponseBuilder(shardEntry, true);
    ShardRequest sreq = getFieldsRequest(new SolrServerException(EXCEPTION_MESSAGE));

    new QueryComponent().returnFields(rb, sreq);

    assertEquals(EXPECTED_ERROR, shardEntry.get("error"));
  }

  // When the SolrServerException has a cause, it is unwrapped to that cause.
  @Test
  public void unwrapsCauseWhenPresent() {
    assumeWorkingMockito();
    SimpleOrderedMap<Object> shardEntry = new SimpleOrderedMap<>();
    ResponseBuilder rb = getFieldsResponseBuilder(shardEntry, true);
    ShardRequest sreq =
        getFieldsRequest(new SolrServerException("wrapper", new RuntimeException("root cause")));

    new QueryComponent().returnFields(rb, sreq);

    assertEquals("java.lang.RuntimeException: root cause", shardEntry.get("error"));
  }

  // When the stack trace is not hidden, the trace branch records a trace alongside the error.
  @Test
  public void recordsTraceWhenStackTraceVisible() {
    assumeWorkingMockito();
    SimpleOrderedMap<Object> shardEntry = new SimpleOrderedMap<>();
    ResponseBuilder rb = getFieldsResponseBuilder(shardEntry, false);
    ShardRequest sreq = getFieldsRequest(new SolrServerException(EXCEPTION_MESSAGE));

    new QueryComponent().returnFields(rb, sreq);

    assertEquals(EXPECTED_ERROR, shardEntry.get("error"));
    Object trace = shardEntry.get("trace");
    assertNotNull("stack trace should be recorded when not hidden", trace);
    assertTrue(
        "trace should contain the exception", trace.toString().contains("SolrServerException"));
  }

  private static ResponseBuilder getFieldsResponseBuilder(
      SimpleOrderedMap<Object> shardEntry, boolean hideStackTrace) {
    // Request: unique key "id", shards.info=true.
    SolrQueryRequest req = Mockito.mock(SolrQueryRequest.class);
    IndexSchema schema = Mockito.mock(IndexSchema.class);
    Mockito.when(req.getSchema()).thenReturn(schema);
    Mockito.when(schema.getUniqueKeyField()).thenReturn(new SchemaField("id", new StrField()));
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(ShardParams.SHARDS_INFO, true);
    Mockito.when(req.getParams()).thenReturn(params);
    SolrCore core = Mockito.mock(SolrCore.class);
    CoreContainer cc = Mockito.mock(CoreContainer.class);
    Mockito.when(req.getCore()).thenReturn(core);
    Mockito.when(core.getCoreContainer()).thenReturn(cc);
    Mockito.when(cc.hideStackTrace()).thenReturn(hideStackTrace);

    // Response: real ReturnFields + a values list already holding a shards.info entry for shard1.
    SolrQueryResponse rsp = Mockito.mock(SolrQueryResponse.class);
    Mockito.when(rsp.getReturnFields()).thenReturn(new SolrReturnFields());
    NamedList<Object> values = new NamedList<>();
    SimpleOrderedMap<Object> shardsInfo = new SimpleOrderedMap<>();
    shardsInfo.add("shard1", shardEntry);
    values.add(ShardParams.SHARDS_INFO, shardsInfo);
    Mockito.when(rsp.getValues()).thenReturn(values);

    return new ResponseBuilder(req, rsp, new ArrayList<>());
  }

  // A GET_FIELDS shard request whose shard failed with the given exception.
  private static ShardRequest getFieldsRequest(Throwable exception) {
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_GET_FIELDS;
    ShardResponse srsp = new ShardResponse();
    srsp.setShard("shard1");
    srsp.setException(exception);
    sreq.responses.add(srsp);
    return sreq;
  }
}

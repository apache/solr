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

  @Test
  public void returnFieldsNpeOnCauselessSolrServerException() {
    assumeWorkingMockito();

    // Request: unique key "id", shards.info=true.
    SolrQueryRequest req = Mockito.mock(SolrQueryRequest.class);
    IndexSchema schema = Mockito.mock(IndexSchema.class);
    Mockito.when(req.getSchema()).thenReturn(schema);
    Mockito.when(schema.getUniqueKeyField()).thenReturn(new SchemaField("id", new StrField()));
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(ShardParams.SHARDS_INFO, true);
    Mockito.when(req.getParams()).thenReturn(params);
    // Needed on the (fixed) path that reaches the hideStackTrace() check.
    SolrCore core = Mockito.mock(SolrCore.class);
    CoreContainer cc = Mockito.mock(CoreContainer.class);
    Mockito.when(req.getCore()).thenReturn(core);
    Mockito.when(core.getCoreContainer()).thenReturn(cc);
    Mockito.when(cc.hideStackTrace()).thenReturn(true);

    // Response: real ReturnFields + a values list already holding a shards.info entry for shard1.
    SolrQueryResponse rsp = Mockito.mock(SolrQueryResponse.class);
    Mockito.when(rsp.getReturnFields()).thenReturn(new SolrReturnFields());
    NamedList<Object> values = new NamedList<>();
    SimpleOrderedMap<Object> shardsInfo = new SimpleOrderedMap<>();
    SimpleOrderedMap<Object> shardEntry = new SimpleOrderedMap<>();
    shardsInfo.add("shard1", shardEntry);
    values.add(ShardParams.SHARDS_INFO, shardsInfo);
    Mockito.when(rsp.getValues()).thenReturn(values);

    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>());

    // A GET_FIELDS shard request whose shard failed with a cause-less SolrServerException.
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_GET_FIELDS;
    ShardResponse srsp = new ShardResponse();
    srsp.setShard("shard1");
    srsp.setException(new SolrServerException("Time allowed to handle this request exceeded"));
    sreq.responses.add(srsp);

    QueryComponent queryComponent = new QueryComponent();

    // Before the SOLR-18596 guard is added here, this call throws NullPointerException at
    // `nl.add("error", t.toString())` because the unwrapped cause is null.
    queryComponent.returnFields(rb, sreq);

    assertNotNull("shard error should be recorded, not NPE", shardEntry.get("error"));
  }
}

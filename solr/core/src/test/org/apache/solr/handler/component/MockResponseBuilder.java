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
import java.util.List;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SortSpec;
import org.mockito.Mockito;

public class MockResponseBuilder extends ResponseBuilder {

  private MockResponseBuilder(
      SolrQueryRequest request, SolrQueryResponse response, List<SearchComponent> components) {
    super(request, response, components);
  }

  public static MockResponseBuilder create() {

    // the mocks
    SolrQueryRequest request = Mockito.mock(SolrQueryRequest.class);
    SolrQueryResponse response = Mockito.mock(SolrQueryResponse.class);
    IndexSchema indexSchema = Mockito.mock(IndexSchema.class);
    SolrParams params = Mockito.mock(SolrParams.class);

    // SchemaField must be concrete due to field access
    SchemaField uniqueIdField = new SchemaField("id", new StrField());

    // we need this because QueryComponent adds a property to it.
    SimpleOrderedMap<Object> responseHeader = new SimpleOrderedMap<>();

    // the mock implementations
    Mockito.when(request.getSchema()).thenReturn(indexSchema);
    Mockito.when(indexSchema.getUniqueKeyField()).thenReturn(uniqueIdField);
    Mockito.when(params.getBool(ShardParams.SHARDS_INFO)).thenReturn(false);
    Mockito.when(request.getParams()).thenReturn(params);
    Mockito.when(response.getResponseHeader()).thenReturn(responseHeader);
    Mockito.when(response.getReturnFields()).thenCallRealMethod();

    List<SearchComponent> components = new ArrayList<>();
    return new MockResponseBuilder(request, response, components);
  }

  public MockResponseBuilder withSortSpec(SortSpec sortSpec) {
    this.setSortSpec(sortSpec);
    return this;
  }

  public MockResponseBuilder withCombinerQueries(String... combinerQueries) {
    SolrParams params = req.getParams();
    Mockito.when(params.getParams(CombinerParams.COMBINER_QUERY)).thenReturn(combinerQueries);
    Mockito.when(params.get(CombinerParams.COMBINER_ALGORITHM, CombinerParams.DEFAULT_COMBINER))
        .thenReturn(CombinerParams.DEFAULT_COMBINER);
    SolrCore core = Mockito.mock(SolrCore.class);
    Mockito.when(core.getCoreContainer()).thenReturn(Mockito.mock(CoreContainer.class));
    Mockito.when(req.getCore()).thenReturn(core);
    Mockito.when(rsp.getReturnFields()).thenReturn(Mockito.mock(ReturnFields.class));
    return this;
  }

  public MockResponseBuilder withShardsTolerant() {
    SolrParams params = req.getParams();
    Mockito.when(params.get(ShardParams.SHARDS_TOLERANT)).thenReturn("true");
    Mockito.when(params.getBool(Mockito.eq(CommonParams.PARTIAL_RESULTS), Mockito.anyBoolean()))
        .thenReturn(true);
    return this;
  }
}

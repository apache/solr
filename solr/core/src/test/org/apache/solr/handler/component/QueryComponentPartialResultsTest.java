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

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.*;
import org.apache.solr.search.SortSpec;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

public class QueryComponentPartialResultsTest extends SolrTestCaseJ4 {
    private static int id = 0;
    private static final String SORT_FIELD_NAME = "category";

    @BeforeClass
    public static void setup() {
        assumeWorkingMockito();
    }

    @Test
    public void includesPartialShardResultWhenUsingImplicitScoreSort() {
        SortSpec sortSpec = MockSortSpecBuilder.create()
                .withIncludesNonScoreOrDocSortField(false)
                .build();
        testPartialResultsForSortSpec(sortSpec, true);
    }

    @Test
    public void includesPartialShardResultWhenUsingExplicitScoreSort() {
        SortSpec sortSpec = MockSortSpecBuilder.create()
                .withSortFields(new SortField[]{SortField.FIELD_SCORE})
                .withIncludesNonScoreOrDocSortField(false)
                .build();
        testPartialResultsForSortSpec(sortSpec, true);
    }

    @Test
    public void includesPartialShardResultWhenUsingExplicitDocSort() {
        SortSpec sortSpec = MockSortSpecBuilder.create()
                .withSortFields(new SortField[]{SortField.FIELD_DOC})
                .withIncludesNonScoreOrDocSortField(false)
                .build();
        testPartialResultsForSortSpec(sortSpec, true);
    }

    @Test
    public void excludesPartialShardResultWhenUsingNonScoreOrDocSortField() {
        SortField sortField = new SortField(SORT_FIELD_NAME, SortField.Type.INT);
        SortSpec sortSpec = MockSortSpecBuilder.create()
                .withSortFields(new SortField[]{sortField})
                .withIncludesNonScoreOrDocSortField(true)
                .build();
        testPartialResultsForSortSpec(sortSpec, false);
    }

    private void testPartialResultsForSortSpec(SortSpec sortSpec, boolean shouldIncludesPartialShardResult) {
        final int shard1Size = 2;
        final int shard2Size = 3;

        MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

        // shard 1 is marked partial
        NamedList<Object> responseHeader1 = new NamedList<>();
        responseHeader1.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);

        // shard 2 is not partial
        NamedList<Object> responseHeader2 = new NamedList<>();

        MockShardRequest shardRequest = MockShardRequest.create()
                .withShardResponse(responseHeader1, createSolrDocumentList(shard1Size))
                .withShardResponse(responseHeader2, createSolrDocumentList(shard2Size));

        QueryComponent queryComponent = new QueryComponent();
        queryComponent.mergeIds(responseBuilder, shardRequest);

        // do we have the expected document count
        assertEquals((shouldIncludesPartialShardResult ? shard1Size : 0) + shard2Size, responseBuilder.getResponseDocs().size());
    }

    private static class MockResponseBuilder extends ResponseBuilder {

        private MockResponseBuilder(SolrQueryRequest request, SolrQueryResponse response, List<SearchComponent> components) {
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
            NamedList<Object> responseHeader = new NamedList<>();

            // the mock implementations
            Mockito.when(request.getSchema()).thenReturn(indexSchema);
            Mockito.when(indexSchema.getUniqueKeyField()).thenReturn(uniqueIdField);
            Mockito.when(params.getBool(ShardParams.SHARDS_INFO)).thenReturn(false);
            Mockito.when(request.getParams()).thenReturn(params);
            Mockito.when(response.getResponseHeader()).thenReturn(responseHeader);

            List<SearchComponent> components = new ArrayList<>();
            return new MockResponseBuilder(request, response, components);

        }

        public MockResponseBuilder withSortSpec(SortSpec sortSpec) {
            this.setSortSpec(sortSpec);
            return this;
        }

    }

    private static class MockShardRequest extends ShardRequest {

        public static MockShardRequest create() {
            MockShardRequest mockShardRequest = new MockShardRequest();
            mockShardRequest.responses = new ArrayList<>();
            return mockShardRequest;
        }

        public MockShardRequest withShardResponse(NamedList<Object> responseHeader, SolrDocumentList solrDocuments) {
            ShardResponse shardResponse = buildShardResponse(responseHeader, solrDocuments);
            responses.add(shardResponse);
            return this;
        }

        private ShardResponse buildShardResponse(NamedList<Object> responseHeader, SolrDocumentList solrDocuments) {
            SolrResponse solrResponse = Mockito.mock(SolrResponse.class);
            ShardResponse shardResponse = new ShardResponse();
            NamedList<Object> response = new NamedList<>();
            response.add("response", solrDocuments);
            shardResponse.setSolrResponse(solrResponse);
            response.add("responseHeader", responseHeader);
            Mockito.when(solrResponse.getResponse()).thenReturn(response);

            return shardResponse;
        }

    }

    private static class MockSortSpecBuilder {
        private final SortSpec sortSpec;

        public MockSortSpecBuilder() {
            this.sortSpec = Mockito.mock(SortSpec.class);
            Mockito.when(sortSpec.getCount()).thenReturn(10);
        }

        public static MockSortSpecBuilder create() {
            return new MockSortSpecBuilder();
        }

        public MockSortSpecBuilder withSortFields(SortField[] sortFields) {
            Sort sort = Mockito.mock(Sort.class);
            Mockito.when(sort.getSort()).thenReturn(sortFields);
            Mockito.when(sortSpec.getSort()).thenReturn(sort);
            return this;
        }

        public MockSortSpecBuilder withIncludesNonScoreOrDocSortField(boolean include) {
            Mockito.when(sortSpec.includesNonScoreOrDocField()).thenReturn(include);
            return this;
        }

        public SortSpec build() {
            return sortSpec;
        }

    }

    private static SolrDocumentList createSolrDocumentList(int size) {
        SolrDocumentList solrDocuments = new SolrDocumentList();
        for(int i = 0; i < size; i++) {
            SolrDocument solrDocument = new SolrDocument();
            solrDocument.addField("id", id++);
            solrDocument.addField("score", id * 1.1F);
            solrDocument.addField(SORT_FIELD_NAME, id * 10);
            solrDocuments.add(solrDocument);
        }
        return solrDocuments;
    }

}

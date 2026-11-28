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
package org.apache.solr.search.grouping.distributed.requestfactory;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;

/**
 * SOLR-8939: the grouping stored-fields request must format a date unique key the same way as the
 * plain distributed path, so the receiving shard can parse it back.
 */
public class StoredFieldsShardRequestFactoryTest extends SolrTestCase {

  public void testDateUniqueKeyIsSentAsIsoWithMilliseconds() {
    ShardRequest[] requests =
        constructRequests(Date.from(Instant.parse("2016-03-21T10:15:30.123Z")));
    assertEquals(1, requests.length);
    assertEquals("2016-03-21T10:15:30.123Z", requests[0].params.get(ShardParams.IDS));
  }

  public void testStringUniqueKeyIsSentAsIs() {
    ShardRequest[] requests = constructRequests("abc");
    assertEquals(1, requests.length);
    assertEquals("abc", requests[0].params.get(ShardParams.IDS));
  }

  private ShardRequest[] constructRequests(Object uniqueId) {
    assumeWorkingMockito();
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    IndexSchema schema = mock(IndexSchema.class);
    when(schema.getUniqueKeyField()).thenReturn(new SchemaField("id", new StrField()));
    when(req.getSchema()).thenReturn(schema);
    when(req.getParams()).thenReturn(new ModifiableSolrParams());

    ResponseBuilder rb = new ResponseBuilder(req, new SolrQueryResponse(), new ArrayList<>());
    ShardDoc doc = new ShardDoc(1.0f, null, uniqueId, "shard1");
    TopDocs topDocs =
        new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {doc});
    rb.mergedQueryCommandResults.put("q", new QueryCommandResult(topDocs, 1, 1.0f));
    return new StoredFieldsShardRequestFactory().constructRequest(rb);
  }
}

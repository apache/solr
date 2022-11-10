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
package org.apache.solr.search;

import java.util.ArrayList;
import java.util.Set;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Test;

/** Tests the utility methods in QueryUtils. */
public class QueryUtilsTest extends SolrTestCaseJ4 {

  @Test
  public void testGetTaggedQueries() throws Exception {
    try {
      initCore("solrconfig.xml", "schema.xml");

      SearchComponent queryComponent =
          h.getCore().getSearchComponent(QueryComponent.COMPONENT_NAME);

      // first make sure QueryUtils.getTaggedQueries() behaves properly on an "empty" request
      try (SolrQueryRequest request = req()) {
        ResponseBuilder rb =
            new ResponseBuilder(request, new SolrQueryResponse(), new ArrayList<>());
        assertEquals(0, QueryUtils.getTaggedQueries(request, Set.of("t0")).size());
        queryComponent.prepare(rb);
        assertEquals(0, QueryUtils.getTaggedQueries(request, Set.of("t0")).size());
      }

      // now test with a request that includes multiple tagged queries
      try (SolrQueryRequest request =
          req(
              CommonParams.Q, "{!tag=t0}text:zzzz",
              CommonParams.FQ, "{!tag=t1,t2}str_s:a",
              CommonParams.FQ, "{!tag=t3}str_s:b",
              CommonParams.FQ, "{!tag=t3}str_s:b",
              CommonParams.FQ, "{!tag=t4}str_s:b",
              CommonParams.FQ, "str_s:b",
              CommonParams.FQ, "str_s:c")) {

        ResponseBuilder rb =
            new ResponseBuilder(request, new SolrQueryResponse(), new ArrayList<>());

        // the "tag map" will not be populated in the SolrQueryRequest's "context" until
        // the QParsers have been initialized; before such time, QueryUtils.getTaggedQueries will
        // always return an empty set
        assertEquals(0, QueryUtils.getTaggedQueries(request, Set.of("t0")).size());

        // initialize QParsers and thereby populate the "tag map"
        queryComponent.prepare(rb);

        // the tag t1000 isn't present in any queries
        assertEquals(0, QueryUtils.getTaggedQueries(request, Set.of("t1000")).size());

        // the main "q" is tagged with t0
        assertEquals(1, QueryUtils.getTaggedQueries(request, Set.of("t0")).size());
        assertEquals(
            "text:zzzz",
            QueryUtils.getTaggedQueries(request, Set.of("t0"))
                .toArray(new Query[] {})[0]
                .toString());

        // the first "fq" is tagged with t1 and t2
        assertEquals(1, QueryUtils.getTaggedQueries(request, Set.of("t1")).size());
        assertEquals(1, QueryUtils.getTaggedQueries(request, Set.of("t2")).size());
        assertEquals(1, QueryUtils.getTaggedQueries(request, Set.of("t1", "t2")).size());
        assertEquals(
            "str_s:a",
            QueryUtils.getTaggedQueries(request, Set.of("t1"))
                .toArray(new Query[] {})[0]
                .toString());

        // here our desired tags span q and fq
        assertEquals(2, QueryUtils.getTaggedQueries(request, Set.of("t0", "t2")).size());

        // there are two fq's tagged with t3 and having the same query "str_s:b"
        // these Queries are equal() but not ==
        // QueryUtils.getTaggedQueries returns a Set that uses reference equality, so it should have
        // 2 elements, not 1
        assertEquals(2, QueryUtils.getTaggedQueries(request, Set.of("t3")).size());
        Query[] queries =
            QueryUtils.getTaggedQueries(request, Set.of("t3")).toArray(new Query[] {});
        assertEquals(queries[0], queries[1]);
        assertNotSame(queries[0], queries[1]);

        assertEquals(1, QueryUtils.getTaggedQueries(request, Set.of("t4")).size());
        assertEquals(3, QueryUtils.getTaggedQueries(request, Set.of("t3", "t4")).size());
        assertEquals(
            5,
            QueryUtils.getTaggedQueries(
                    request, Set.of("t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7"))
                .size());
      }
    } finally {
      deleteCore();
    }
  }
}

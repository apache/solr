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

import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_NEXT;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_PARAM;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_START;
import static org.apache.solr.common.util.Utils.fromJSONString;

import java.io.File;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.QueryElevationParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.query.FilterQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.CollapsingQParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.WrappedQuery;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryElevationComponentTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() {
    switch (random().nextInt(3)) {
      case 0:
        System.setProperty("solr.tests.id.stored", "true");
        System.setProperty("solr.tests.id.docValues", "true");
        break;
      case 1:
        System.setProperty("solr.tests.id.stored", "true");
        System.setProperty("solr.tests.id.docValues", "false");
        break;
      case 2:
        System.setProperty("solr.tests.id.stored", "false");
        System.setProperty("solr.tests.id.docValues", "true");
        break;
      default:
        fail("Bad random number generated not between 0-2 inclusive");
        break;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  private void init(String schema) throws Exception {
    initCore("solrconfig-elevate.xml", schema);
    clearIndex();
    assertU(commit());
  }

  // TODO should be @After ?
  private void delete() {
    deleteCore();
  }

  @Test
  public void testFieldType() throws Exception {
    try {
      init("schema11.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "1", "text", "XXXX XXXX", "str_s", "a"));
      assertU(adoc("id", "2", "text", "YYYY", "str_s", "b"));
      assertU(adoc("id", "3", "text", "ZZZZ", "str_s", "c"));

      assertU(adoc("id", "4", "text", "XXXX XXXX", "str_s", "x"));
      assertU(adoc("id", "5", "text", "YYYY YYYY", "str_s", "y"));
      assertU(adoc("id", "6", "text", "XXXX XXXX", "str_s", "z"));
      assertU(adoc("id", "7", "text", "AAAA", "str_s", "a"));
      assertU(adoc("id", "8", "text", "AAAA", "str_s", "a"));
      assertU(adoc("id", "9", "text", "AAAA AAAA", "str_s", "a"));
      assertU(commit());

      assertQ(
          "",
          req(
              CommonParams.Q,
              "AAAA",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='7']",
          "//result/doc[2]/str[@name='id'][.='9']",
          "//result/doc[3]/str[@name='id'][.='8']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='false']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']");
    } finally {
      delete();
    }
  }

  /**
   * Tests the behavior of elevation when the fq parameter is present: elevated documents are
   * subject to filters unless specific filters have been tagged for exclusion.
   */
  @Test
  public void testFq() throws Exception {
    try {
      init("schema11.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "1", "text", "XXXX", "str_s", "a"));
      assertU(adoc("id", "2", "text", "YYYY", "str_s", "b"));
      assertU(adoc("id", "3", "text", "QQQQ", "str_s", "c"));
      assertU(adoc("id", "4", "text", "MMMM", "str_s", "d"));
      assertU(commit());

      // elevated docs 1, 2, and 3 are returned even though our query "ZZZZ" doesn't match them
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='3']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']");

      // the elevation component respects the fq parameter; if we add fq=str_s:b the results will
      // exclude docs 1 and 3 even though those docs are elevated
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "str_s:b"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      // if we tag the filter without also specifying the tag for exclusion,
      // we should see the same behavior as above; the filter still takes effect on the elevated
      // docs
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1,test2}str_s:b",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test3"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      // if we tag the filter without also specifying the tag for exclusion, we should see the same
      // behavior as above; the filter still takes effect on the elevated docs
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1,test2}str_s:b",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, ","),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      // if we specify at least one of the filter's tags for exclusion, the filter is broadened to
      // include the elvated docs; we should see docs 1 and 3 returned even though they don't match
      // the original filter
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1,test2}str_s:b",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test0,test2,test4"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='3']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']");

      // redundant tags don't cause problems; nor does tagging something that's not a filter (in
      // this case, the main query); nor does including empty values in the list of tags to exclude
      assertQ(
          "",
          req(
              CommonParams.Q, "{!tag=test0}ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1,test1,test2,test2}str_s:b",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test0,test0,test2,test2,test4,test4,,,"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='3']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']");

      // we can exclude some filters while leaving others in place
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1}id:10",
              CommonParams.FQ, "{!tag=test2}str_s:b",
              CommonParams.FQ, "{!tag=test3}id:11",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1,test3"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      // when filters are marked as cache=false, tag exclusion works the same as before
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1 cache=false}id:10",
              CommonParams.FQ, "{!tag=test2 cache=false}str_s:b",
              CommonParams.FQ, "{!tag=test3 cache=false}id:11",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1,test3"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      // we can apply the same tag to two different filters
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1}id:10",
              CommonParams.FQ, "{!tag=test2}str_s:b",
              CommonParams.FQ, "{!tag=test1}id:11",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1,test3"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      // we can use filter() syntax inside fq's that are tagged for exclusion
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1}+filter(id:10) +filter(id:11)",
              CommonParams.FQ, "{!tag=test2}filter(str_s:b)",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      // the next few assertions confirm that filter exclusion only applies to elevated documents;
      // if we search for MMMM we should get one match; no documents are elevated for this query
      assertQ(
          "",
          req(
              CommonParams.Q, "MMMM",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='4']",
          "//result/doc[1]/bool[@name='[elevated]'][.='false']");

      // if we add fq=str_s:b, our one document that matches MMMM will be filtered out
      assertQ(
          "",
          req(
              CommonParams.Q, "MMMM",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "str_s:b"),
          "//*[@numFound='0']");

      // if we tag the filter and exclude it, we should see the same behavior as before; filters are
      // only bypassed for elevated documents; our MMMM document is not elevated so it is still
      // subject to the filter
      assertQ(
          "",
          req(
              CommonParams.Q, "MMMM",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!tag=test1}str_s:b",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1"),
          "//*[@numFound='0']");

      // the next few assertions confirm that collapsing works as expected when filters are
      // excluded; first, confirm that when collapsing, all elevated docs are visible by default
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!collapse field=str_s sort='score desc'}"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='3']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']");

      // when collapsing, an added filter has the expected effect
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!collapse field=str_s sort='score desc'}",
              CommonParams.FQ, "str_s:b"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      // when collapsing, filters can still be tagged and excluded so that they don't affect
      // elevated documents
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!collapse field=str_s sort='score desc'}",
              CommonParams.FQ, "{!tag=test1}str_s:b",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='3']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']");

      // if a collapse filter itself is tagged for exclusion, this is considered an error and the
      // user should be informed
      assertQEx(
          "tagging a collapse filter for exclusion should lead to a BAD_REQUEST",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!collapse tag=test1 field=str_s sort='score desc'}",
              CommonParams.FQ, "{!tag=test2}str_s:b",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1,test2"),
          SolrException.ErrorCode.BAD_REQUEST);

      // if a function range query is provided as a filter, it can be tagged for exclusion;
      // FunctionRangeQuery is special because it implements the PostFilter interface and
      // we want to be sure that while CollapsingPostFilter leads to an error, other PostFilters
      // can still be used; here we set cache=false and cost=200 to trigger the post-filtering
      // behavior
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "{!frange tag=test1 l=100 cache=false cost=200}5.0",
              CommonParams.FQ, "{!tag=test2}str_s:b",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1,test2"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='3']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']");

    } finally {
      delete();
    }
  }

  /**
   * Tests that QueryElevationComponent's prepare() method transforms filters as expected. By
   * default, filters are left unchanged. However, if a filter is tagged for exclusion it is
   * transformed into a BooleanQuery containing the original filter and an "include query" that
   * matches the elevated documents. The original filter will be wrapped in a FilterQuery if the
   * original filter was cacheable and not already a FilterQuery.
   */
  @Test
  public void testFqWithCacheAndCostLocalParams() throws Exception {
    try {
      init("schema11.xml");
      SearchComponent queryComponent =
          h.getCore().getSearchComponent(QueryComponent.COMPONENT_NAME);
      QueryElevationComponent elevationComponent =
          (QueryElevationComponent) h.getCore().getSearchComponent("elevate");

      // first, we establish the baseline behavior that occurs when executing a query that is NOT
      // elevated; when a filter like "str_s:A" has no local params, it will generate a TermQuery;
      // when a filter uses the "cache" and/or "cost" local params, it will generate a WrappedQuery
      // which contains a TermQuery and which has its cache and cost set according to the param
      // values
      try (SolrQueryRequest request =
          req(
              CommonParams.Q, "ZZZZ1",
              CommonParams.QT, "/elevate",
              CommonParams.DF, "text",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "str_s:A",
              CommonParams.FQ, "{!cache=false cost=100}str_s:B",
              CommonParams.FQ, "{!cache=true}str_s:C",
              CommonParams.FQ, "{!tag=test1 cache=false cost=200}str_s:D",
              CommonParams.FQ, "{!tag=test1 cache=true}str_s:E",
              CommonParams.FQ, "{!tag=test1}str_s:F",
              CommonParams.FQ, "{!tag=test1}filter(str_s:G)",
              CommonParams.FQ, "{!tag=test1 cache=true cost=10}filter(str_s:H)",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1")) {

        // create a ResponseBuilder with our query and pass it through the prepare() methods of the
        // QueryComponent and the QueryElevationComponent
        ResponseBuilder rb =
            new ResponseBuilder(request, new SolrQueryResponse(), new ArrayList<>());
        queryComponent.prepare(rb);
        elevationComponent.prepare(rb);

        assertNull(elevationComponent.getElevation(rb));

        Query[] filters = rb.getFilters().toArray(new Query[0]);
        assertEquals(8, filters.length);

        // str_s:A
        assertTrue(filters[0] instanceof TermQuery);

        // {!cache=false cost=100}str_s:B
        assertTrue(filters[1] instanceof WrappedQuery);
        assertFalse(((WrappedQuery) filters[1]).getCache());
        assertEquals(100, ((WrappedQuery) filters[1]).getCost());
        assertTrue(((WrappedQuery) filters[1]).getWrappedQuery() instanceof TermQuery);

        // {!cache=true}str_s:C
        assertTrue(filters[2] instanceof WrappedQuery);
        assertTrue(((WrappedQuery) filters[2]).getCache());
        assertTrue(((WrappedQuery) filters[2]).getWrappedQuery() instanceof TermQuery);

        // {!tag=test1 cache=false cost=200}str_s:D
        assertTrue(filters[3] instanceof WrappedQuery);
        assertFalse(((WrappedQuery) filters[3]).getCache());
        assertEquals(200, ((WrappedQuery) filters[3]).getCost());
        assertTrue(((WrappedQuery) filters[3]).getWrappedQuery() instanceof TermQuery);

        // {!tag=test1 cache=true}str_s:E
        assertTrue(filters[4] instanceof WrappedQuery);
        assertTrue(((WrappedQuery) filters[4]).getCache());
        assertTrue(((WrappedQuery) filters[4]).getWrappedQuery() instanceof TermQuery);

        // {!tag=test1}str_s:F
        assertTrue(filters[5] instanceof TermQuery);

        // {!tag=test1}filter(str_s:G)
        assertTrue(filters[6] instanceof FilterQuery);

        // {!tag=test1 cache=false}filter(str_s:H)
        assertTrue(filters[7] instanceof FilterQuery);
      }

      // now establish that when a query IS elevated, non-excluded filters behave the same as shown
      // above; however, excluded filters always generate a non-caching WrappedQuery that contains a
      // BooleanQuery; the first clause of the BooleanQuery will be the original filter, if the
      // original filter had cache=false or was a FilterQuery e.g. defined using filter() syntax; if
      // the original filter was cacheable, the first clause will be FilterQuery containing the
      // original filter; if the original filter _was_ a FilterQuery it is not wrapped in another
      // one
      try (SolrQueryRequest request =
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CommonParams.DF, "text",
              CommonParams.FL, "id, score, [elevated]",
              CommonParams.FQ, "str_s:A",
              CommonParams.FQ, "{!cache=false cost=100}str_s:B",
              CommonParams.FQ, "{!cache=true}str_s:C",
              CommonParams.FQ, "{!tag=test1 cache=false cost=200}str_s:D",
              CommonParams.FQ, "{!tag=test1 cache=true}str_s:E",
              CommonParams.FQ, "{!tag=test1}str_s:F",
              CommonParams.FQ, "{!tag=test1}filter(str_s:G)",
              CommonParams.FQ, "{!tag=test1 cache=false}filter(str_s:H)",
              QueryElevationParams.ELEVATE_EXCLUDE_TAGS, "test1")) {

        ResponseBuilder rb =
            new ResponseBuilder(request, new SolrQueryResponse(), new ArrayList<>());
        queryComponent.prepare(rb);
        elevationComponent.prepare(rb);

        assertEquals(3, elevationComponent.getElevation(rb).elevatedIds.size());

        Query[] filters = rb.getFilters().toArray(new Query[0]);
        assertEquals(8, filters.length);

        // str_s:A
        assertTrue(filters[0] instanceof TermQuery);

        // {!cache=false cost=100}str_s:B
        assertTrue(filters[1] instanceof WrappedQuery);
        assertFalse(((WrappedQuery) filters[1]).getCache());
        assertEquals(100, ((WrappedQuery) filters[1]).getCost());
        assertTrue(((WrappedQuery) filters[1]).getWrappedQuery() instanceof TermQuery);

        // {!cache=true}str_s:C
        assertTrue(filters[2] instanceof WrappedQuery);
        assertTrue(((WrappedQuery) filters[2]).getCache());
        assertTrue(((WrappedQuery) filters[2]).getWrappedQuery() instanceof TermQuery);

        // {!tag=test1 cache=false cost=200}str_s:D
        {
          // at the outermost level, we have a WrappedQuery that doesn't cache
          assertTrue(filters[3] instanceof WrappedQuery);
          assertFalse(((WrappedQuery) filters[3]).getCache());
          assertEquals(200, ((WrappedQuery) filters[3]).getCost());
          // inside the WrappedQuery, there's a BooleanQuery with two clauses
          BooleanQuery booleanQuery = (BooleanQuery) ((WrappedQuery) filters[3]).getWrappedQuery();
          assertEquals(2, booleanQuery.clauses().size());
          // the first clause is the original query, which is a WrappedQuery because the cache=false
          // local param was provided; the WrappedQuery doesn't cache; it contains a TermQuery
          Query firstClause = booleanQuery.clauses().get(0).getQuery();
          assertTrue(firstClause instanceof WrappedQuery);
          assertFalse(((WrappedQuery) firstClause).getCache());
          assertTrue(((WrappedQuery) firstClause).getWrappedQuery() instanceof TermQuery);
        }

        // {!tag=test1 cache=true}str_s:E
        {
          // at the outermost level, we have a WrappedQuery that doesn't cache
          assertTrue(filters[4] instanceof WrappedQuery);
          assertFalse(((WrappedQuery) filters[4]).getCache());
          // inside the WrappedQuery, there's a BooleanQuery with two clauses
          BooleanQuery booleanQuery = (BooleanQuery) ((WrappedQuery) filters[4]).getWrappedQuery();
          assertEquals(2, booleanQuery.clauses().size());
          // the first clause is a FilterQuery that contains the original query
          // (ElevateComponent introduces this FilterQuery to make sure the original query consults
          // the cache)
          Query firstClause = booleanQuery.clauses().get(0).getQuery();
          assertTrue(firstClause instanceof FilterQuery);
          FilterQuery filterQuery = (FilterQuery) firstClause;
          // the first clause is the original query, which is a WrappedQuery because the cache=false
          // local param was provided; the WrappedQuery is set to cache; it contains a TermQuery
          assertTrue(filterQuery.getQuery() instanceof WrappedQuery);
          assertTrue(((WrappedQuery) filterQuery.getQuery()).getCache());
          assertTrue(
              ((WrappedQuery) filterQuery.getQuery()).getWrappedQuery() instanceof TermQuery);
        }

        // {!tag=test1}str_s:F
        {
          // at the outermost level, we have a WrappedQuery that doesn't cache
          assertTrue(filters[5] instanceof WrappedQuery);
          assertFalse(((WrappedQuery) filters[5]).getCache());
          // inside the WrappedQuery there's a BooleanQuery with two clauses
          BooleanQuery booleanQuery = (BooleanQuery) ((WrappedQuery) filters[5]).getWrappedQuery();
          assertEquals(2, booleanQuery.clauses().size());
          // the first clause is a FilterQuery that contains the original query
          Query firstClause = booleanQuery.clauses().get(0).getQuery();
          assertTrue(firstClause instanceof FilterQuery);
          FilterQuery filterQuery = (FilterQuery) firstClause;
          // the original query is a TermQuery
          assertTrue(filterQuery.getQuery() instanceof TermQuery);
        }

        // {!tag=test1}filter(str_s:G)
        {
          // at the outermost level, we have a WrappedQuery that doesn't cache
          assertTrue(filters[6] instanceof WrappedQuery);
          assertFalse(((WrappedQuery) filters[6]).getCache());
          // inside the WrappedQuery there's a BooleanQuery with two clauses
          BooleanQuery booleanQuery = (BooleanQuery) ((WrappedQuery) filters[6]).getWrappedQuery();
          assertEquals(2, booleanQuery.clauses().size());
          // the first clause is the original query, which is a FilterQuery containing a TermQuery
          Query firstClause = booleanQuery.clauses().get(0).getQuery();
          assertTrue(firstClause instanceof FilterQuery);
          FilterQuery filterQuery = (FilterQuery) firstClause;
          assertTrue(filterQuery.getQuery() instanceof TermQuery);
        }

        // {!tag=test1 cache=false}filter(str_s:H)
        {
          // at the outermost level, we have a WrappedQuery that doesn't cache
          assertTrue(filters[7] instanceof WrappedQuery);
          assertFalse(((WrappedQuery) filters[7]).getCache());
          // inside the WrappedQuery there's a BooleanQuery with two clauses
          BooleanQuery booleanQuery = (BooleanQuery) ((WrappedQuery) filters[7]).getWrappedQuery();
          assertEquals(2, booleanQuery.clauses().size());
          // the first clause is the original query, which is a FilterQuery containing a TermQuery
          Query firstClause = booleanQuery.clauses().get(0).getQuery();
          assertTrue(firstClause instanceof FilterQuery);
          FilterQuery filterQuery = (FilterQuery) firstClause;
          assertTrue(filterQuery.getQuery() instanceof TermQuery);
        }
      }
    } finally {
      delete();
    }
  }

  @Test
  public void testGroupedQuery() throws Exception {
    try {
      init("schema11.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "1", "text", "XXXX XXXX", "str_s", "a"));
      assertU(adoc("id", "2", "text", "XXXX AAAA", "str_s", "b"));
      assertU(adoc("id", "3", "text", "ZZZZ", "str_s", "c"));
      assertU(adoc("id", "4", "text", "XXXX ZZZZ", "str_s", "d"));
      assertU(adoc("id", "5", "text", "ZZZZ ZZZZ", "str_s", "e"));
      assertU(adoc("id", "6", "text", "AAAA AAAA AAAA", "str_s", "f"));
      assertU(adoc("id", "7", "text", "AAAA AAAA ZZZZ", "str_s", "g"));
      assertU(adoc("id", "8", "text", "XXXX", "str_s", "h"));
      assertU(adoc("id", "9", "text", "YYYY ZZZZ", "str_s", "i"));

      assertU(adoc("id", "22", "text", "XXXX ZZZZ AAAA", "str_s", "b"));
      assertU(adoc("id", "66", "text", "XXXX ZZZZ AAAA", "str_s", "f"));
      assertU(adoc("id", "77", "text", "XXXX ZZZZ AAAA", "str_s", "g"));

      assertU(commit());

      final String groups = "//arr[@name='groups']";

      assertQ(
          "non-elevated group query",
          req(
              CommonParams.Q, "AAAA",
              CommonParams.QT, "/elevate",
              GroupParams.GROUP_FIELD, "str_s",
              GroupParams.GROUP, "true",
              GroupParams.GROUP_TOTAL_COUNT, "true",
              GroupParams.GROUP_LIMIT, "100",
              QueryElevationParams.ENABLE, "false",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@name='ngroups'][.='3']",
          "//*[@name='matches'][.='6']",
          groups + "/lst[1]//doc[1]/str[@name='id'][.='6']",
          groups + "/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[1]//doc[2]/str[@name='id'][.='66']",
          groups + "/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[1]/str[@name='id'][.='7']",
          groups + "/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[2]/str[@name='id'][.='77']",
          groups + "/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[1]/str[@name='id'][.='2']",
          groups + "/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[2]/str[@name='id'][.='22']",
          groups + "/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']");

      assertQ(
          "elevated group query",
          req(
              CommonParams.Q, "AAAA",
              CommonParams.QT, "/elevate",
              GroupParams.GROUP_FIELD, "str_s",
              GroupParams.GROUP, "true",
              GroupParams.GROUP_TOTAL_COUNT, "true",
              GroupParams.GROUP_LIMIT, "100",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@name='ngroups'][.='3']",
          "//*[@name='matches'][.='6']",
          groups + "/lst[1]//doc[1]/str[@name='id'][.='7']",
          groups + "/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']",
          groups + "/lst[1]//doc[2]/str[@name='id'][.='77']",
          groups + "/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[1]/str[@name='id'][.='6']",
          groups + "/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[2]/str[@name='id'][.='66']",
          groups + "/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[1]/str[@name='id'][.='2']",
          groups + "/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[2]/str[@name='id'][.='22']",
          groups + "/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']");

      assertQ(
          "non-elevated because sorted group query",
          req(
              CommonParams.Q, "AAAA",
              CommonParams.QT, "/elevate",
              CommonParams.SORT, "id asc",
              GroupParams.GROUP_FIELD, "str_s",
              GroupParams.GROUP, "true",
              GroupParams.GROUP_TOTAL_COUNT, "true",
              GroupParams.GROUP_LIMIT, "100",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@name='ngroups'][.='3']",
          "//*[@name='matches'][.='6']",
          groups + "/lst[1]//doc[1]/str[@name='id'][.='2']",
          groups + "/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[1]//doc[2]/str[@name='id'][.='22']",
          groups + "/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[1]/str[@name='id'][.='6']",
          groups + "/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[2]/str[@name='id'][.='66']",
          groups + "/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[1]/str[@name='id'][.='7']",
          groups + "/lst[3]//doc[1]/bool[@name='[elevated]'][.='true']",
          groups + "/lst[3]//doc[2]/str[@name='id'][.='77']",
          groups + "/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']");

      assertQ(
          "force-elevated sorted group query",
          req(
              CommonParams.Q, "AAAA",
              CommonParams.QT, "/elevate",
              CommonParams.SORT, "id asc",
              QueryElevationParams.FORCE_ELEVATION, "true",
              GroupParams.GROUP_FIELD, "str_s",
              GroupParams.GROUP, "true",
              GroupParams.GROUP_TOTAL_COUNT, "true",
              GroupParams.GROUP_LIMIT, "100",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@name='ngroups'][.='3']",
          "//*[@name='matches'][.='6']",
          groups + "/lst[1]//doc[1]/str[@name='id'][.='7']",
          groups + "/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']",
          groups + "/lst[1]//doc[2]/str[@name='id'][.='77']",
          groups + "/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[1]/str[@name='id'][.='2']",
          groups + "/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[2]/str[@name='id'][.='22']",
          groups + "/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[1]/str[@name='id'][.='6']",
          groups + "/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[2]/str[@name='id'][.='66']",
          groups + "/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']");

      assertQ(
          "non-elevated because of sort within group query",
          req(
              CommonParams.Q, "AAAA",
              CommonParams.QT, "/elevate",
              CommonParams.SORT, "id asc",
              GroupParams.GROUP_SORT, "id desc",
              GroupParams.GROUP_FIELD, "str_s",
              GroupParams.GROUP, "true",
              GroupParams.GROUP_TOTAL_COUNT, "true",
              GroupParams.GROUP_LIMIT, "100",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@name='ngroups'][.='3']",
          "//*[@name='matches'][.='6']",
          groups + "/lst[1]//doc[1]/str[@name='id'][.='22']",
          groups + "/lst[1]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[1]//doc[2]/str[@name='id'][.='2']",
          groups + "/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[1]/str[@name='id'][.='66']",
          groups + "/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[2]/str[@name='id'][.='6']",
          groups + "/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[1]/str[@name='id'][.='77']",
          groups + "/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[2]/str[@name='id'][.='7']",
          groups + "/lst[3]//doc[2]/bool[@name='[elevated]'][.='true']");

      assertQ(
          "force elevated sort within sorted group query",
          req(
              CommonParams.Q, "AAAA",
              CommonParams.QT, "/elevate",
              CommonParams.SORT, "id asc",
              GroupParams.GROUP_SORT, "id desc",
              QueryElevationParams.FORCE_ELEVATION, "true",
              GroupParams.GROUP_FIELD, "str_s",
              GroupParams.GROUP, "true",
              GroupParams.GROUP_TOTAL_COUNT, "true",
              GroupParams.GROUP_LIMIT, "100",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@name='ngroups'][.='3']",
          "//*[@name='matches'][.='6']",
          groups + "/lst[1]//doc[1]/str[@name='id'][.='7']",
          groups + "/lst[1]//doc[1]/bool[@name='[elevated]'][.='true']",
          groups + "/lst[1]//doc[2]/str[@name='id'][.='77']",
          groups + "/lst[1]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[1]/str[@name='id'][.='22']",
          groups + "/lst[2]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[2]//doc[2]/str[@name='id'][.='2']",
          groups + "/lst[2]//doc[2]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[1]/str[@name='id'][.='66']",
          groups + "/lst[3]//doc[1]/bool[@name='[elevated]'][.='false']",
          groups + "/lst[3]//doc[2]/str[@name='id'][.='6']",
          groups + "/lst[3]//doc[2]/bool[@name='[elevated]'][.='false']");

    } finally {
      delete();
    }
  }

  @Test
  public void testTrieFieldType() throws Exception {
    try {
      init("schema.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "1", "text", "XXXX XXXX", "str_s", "a"));
      assertU(adoc("id", "2", "text", "YYYY", "str_s", "b"));
      assertU(adoc("id", "3", "text", "ZZZZ", "str_s", "c"));

      assertU(adoc("id", "4", "text", "XXXX XXXX", "str_s", "x"));
      assertU(adoc("id", "5", "text", "YYYY YYYY", "str_s", "y"));
      assertU(adoc("id", "6", "text", "XXXX XXXX", "str_s", "z"));
      assertU(adoc("id", "7", "text", "AAAA", "str_s", "a"));
      assertU(adoc("id", "8", "text", "AAAA", "str_s", "a"));
      assertU(adoc("id", "9", "text", "AAAA AAAA", "str_s", "a"));
      assertU(commit());

      assertQ(
          "",
          req(
              CommonParams.Q,
              "AAAA",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='7']",
          "//result/doc[2]/str[@name='id'][.='8']",
          "//result/doc[3]/str[@name='id'][.='9']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='false']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']");
    } finally {
      delete();
    }
  }

  @Test
  public void testInterface() throws Exception {
    try {
      init("schema12.xml");
      SolrCore core = h.getCore();

      NamedList<String> args = new NamedList<>();
      args.add(QueryElevationComponent.FIELD_TYPE, "string");
      args.add(QueryElevationComponent.CONFIG_FILE, "elevate.xml");

      IndexReader reader;
      try (SolrQueryRequest req = req()) {
        reader = req.getSearcher().getIndexReader();
      }

      try (QueryElevationComponent comp = new QueryElevationComponent()) {
        comp.init(args);
        comp.inform(core);

        QueryElevationComponent.ElevationProvider elevationProvider =
            comp.getElevationProvider(reader, core);

        // Make sure the boosts loaded properly
        assertEquals(11, elevationProvider.size());
        assertEquals(1, elevationProvider.getElevationForQuery("XXXX").elevatedIds.size());
        assertEquals(2, elevationProvider.getElevationForQuery("YYYY").elevatedIds.size());
        assertEquals(3, elevationProvider.getElevationForQuery("ZZZZ").elevatedIds.size());
        assertNull(elevationProvider.getElevationForQuery("xxxx"));
        assertNull(elevationProvider.getElevationForQuery("yyyy"));
        assertNull(elevationProvider.getElevationForQuery("zzzz"));
      }

      // Now test the same thing with a lowercase filter: 'lowerfilt'
      args = new NamedList<>();
      args.add(QueryElevationComponent.FIELD_TYPE, "lowerfilt");
      args.add(QueryElevationComponent.CONFIG_FILE, "elevate.xml");

      try (QueryElevationComponent comp = new QueryElevationComponent()) {
        comp.init(args);
        comp.inform(core);
        QueryElevationComponent.ElevationProvider elevationProvider =
            comp.getElevationProvider(reader, core);
        assertEquals(11, elevationProvider.size());
        assertEquals(1, elevationProvider.getElevationForQuery("XXXX").elevatedIds.size());
        assertEquals(2, elevationProvider.getElevationForQuery("YYYY").elevatedIds.size());
        assertEquals(3, elevationProvider.getElevationForQuery("ZZZZ").elevatedIds.size());
        assertEquals(1, elevationProvider.getElevationForQuery("xxxx").elevatedIds.size());
        assertEquals(2, elevationProvider.getElevationForQuery("yyyy").elevatedIds.size());
        assertEquals(3, elevationProvider.getElevationForQuery("zzzz").elevatedIds.size());

        assertEquals("xxxx", comp.analyzeQuery("XXXX"));
        assertEquals("xxxxyyyy", comp.analyzeQuery("XXXX YYYY"));

        assertQ(
            "Make sure QEC handles null queries",
            req("qt", "/elevate", "q.alt", "*:*", "defType", "dismax"),
            "//*[@numFound='0']");
      }
    } finally {
      delete();
    }
  }

  @Test
  public void testMarker() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "1", "title", "XXXX XXXX", "str_s1", "a"));
      assertU(adoc("id", "2", "title", "YYYY", "str_s1", "b"));
      assertU(adoc("id", "3", "title", "ZZZZ", "str_s1", "c"));

      assertU(adoc("id", "4", "title", "XXXX XXXX", "str_s1", "x"));
      assertU(adoc("id", "5", "title", "YYYY YYYY", "str_s1", "y"));
      assertU(adoc("id", "6", "title", "XXXX XXXX", "str_s1", "z"));
      assertU(adoc("id", "7", "title", "AAAA", "str_s1", "a"));
      assertU(commit());

      assertQ(
          "",
          req(
              CommonParams.Q,
              "XXXX",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='4']",
          "//result/doc[3]/str[@name='id'][.='6']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='false']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']");

      assertQ(
          "",
          req(
              CommonParams.Q,
              "AAAA",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='7']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");

      assertQ(
          "",
          req(
              CommonParams.Q,
              "AAAA",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elev]"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='7']",
          "not(//result/doc[1]/bool[@name='[elevated]'][.='false'])",
          "not(//result/doc[1]/bool[@name='[elev]'][.='false'])"
          // even though we asked for elev, there is no Transformer registered w/ that, so we
          // shouldn't get a result
          );
    } finally {
      delete();
    }
  }

  @Test
  public void testMarkExcludes() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "1", "title", "XXXX XXXX", "str_s1", "a"));
      assertU(adoc("id", "2", "title", "YYYY", "str_s1", "b"));
      assertU(adoc("id", "3", "title", "ZZZZ", "str_s1", "c"));

      assertU(adoc("id", "4", "title", "XXXX XXXX", "str_s1", "x"));
      assertU(adoc("id", "5", "title", "YYYY YYYY", "str_s1", "y"));
      assertU(adoc("id", "6", "title", "XXXX XXXX", "str_s1", "z"));
      assertU(adoc("id", "7", "title", "AAAA", "str_s1", "a"));

      assertU(adoc("id", "8", "title", " QQQQ trash trash", "str_s1", "q"));
      assertU(adoc("id", "9", "title", " QQQQ QQQQ  trash", "str_s1", "r"));
      assertU(adoc("id", "10", "title", "QQQQ QQQQ  QQQQ ", "str_s1", "s"));

      assertU(commit());

      assertQ(
          "",
          req(
              CommonParams.Q,
              "XXXX XXXX",
              CommonParams.QT,
              "/elevate",
              QueryElevationParams.MARK_EXCLUDES,
              "true",
              "indent",
              "true",
              CommonParams.FL,
              "id, score, [excluded]"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='5']",
          "//result/doc[2]/str[@name='id'][.='1']",
          "//result/doc[3]/str[@name='id'][.='4']",
          "//result/doc[4]/str[@name='id'][.='6']",
          "//result/doc[1]/bool[@name='[excluded]'][.='false']",
          "//result/doc[2]/bool[@name='[excluded]'][.='false']",
          "//result/doc[3]/bool[@name='[excluded]'][.='false']",
          "//result/doc[4]/bool[@name='[excluded]'][.='true']");

      // ask for excluded as a field, but don't actually request the MARK_EXCLUDES
      // thus, number 6 should not be returned, b/c it is excluded
      assertQ(
          "",
          req(
              CommonParams.Q,
              "XXXX XXXX",
              CommonParams.QT,
              "/elevate",
              QueryElevationParams.MARK_EXCLUDES,
              "false",
              CommonParams.FL,
              "id, score, [excluded]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='5']",
          "//result/doc[2]/str[@name='id'][.='1']",
          "//result/doc[3]/str[@name='id'][.='4']",
          "//result/doc[1]/bool[@name='[excluded]'][.='false']",
          "//result/doc[2]/bool[@name='[excluded]'][.='false']",
          "//result/doc[3]/bool[@name='[excluded]'][.='false']");

      // test that excluded results are on the same positions in the result list
      // as when elevation component is disabled
      // (i.e. test that elevation component with MARK_EXCLUDES does not boost
      // excluded results)
      assertQ(
          "",
          req(
              CommonParams.Q,
              "QQQQ",
              CommonParams.QT,
              "/elevate",
              QueryElevationParams.ENABLE,
              "false",
              "indent",
              "true",
              CommonParams.FL,
              "id, score"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='10']",
          "//result/doc[2]/str[@name='id'][.='9']",
          "//result/doc[3]/str[@name='id'][.='8']");
      assertQ(
          "",
          req(
              CommonParams.Q,
              "QQQQ",
              CommonParams.QT,
              "/elevate",
              QueryElevationParams.MARK_EXCLUDES,
              "true",
              "indent",
              "true",
              CommonParams.FL,
              "id, score, [excluded]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='10']",
          "//result/doc[2]/str[@name='id'][.='9']",
          "//result/doc[3]/str[@name='id'][.='8']",
          "//result/doc[1]/bool[@name='[excluded]'][.='true']",
          "//result/doc[2]/bool[@name='[excluded]'][.='false']",
          "//result/doc[3]/bool[@name='[excluded]'][.='false']");
    } finally {
      delete();
    }
  }

  @Test
  public void testSorting() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "a", "title", "ipod trash trash", "str_s1", "group1"));
      assertU(adoc("id", "b", "title", "ipod ipod  trash", "str_s1", "group2"));
      assertU(adoc("id", "c", "title", "ipod ipod  ipod ", "str_s1", "group2"));

      assertU(adoc("id", "x", "title", "boosted", "str_s1", "group1"));
      assertU(adoc("id", "y", "title", "boosted boosted", "str_s1", "group2"));
      assertU(adoc("id", "z", "title", "boosted boosted boosted", "str_s1", "group2"));
      assertU(commit());

      final String query = "title:ipod";

      final SolrParams baseParams =
          params(
              "qt", "/elevate",
              "q", query,
              "fl", "id,score",
              "indent", "true");

      QueryElevationComponent booster =
          (QueryElevationComponent) h.getCore().getSearchComponent("elevate");
      IndexReader reader = h.getCore().withSearcher(SolrIndexSearcher::getIndexReader);

      assertQ(
          "Make sure standard sort works as expected",
          req(baseParams),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='c']",
          "//result/doc[2]/str[@name='id'][.='b']",
          "//result/doc[3]/str[@name='id'][.='a']");

      // Explicitly set what gets boosted
      booster.setTopQueryResults(reader, query, false, new String[] {"x", "y", "z"}, null);

      assertQ(
          "All six should make it",
          req(baseParams),
          "//*[@numFound='6']",
          "//result/doc[1]/str[@name='id'][.='x']",
          "//result/doc[2]/str[@name='id'][.='y']",
          "//result/doc[3]/str[@name='id'][.='z']",
          "//result/doc[4]/str[@name='id'][.='c']",
          "//result/doc[5]/str[@name='id'][.='b']",
          "//result/doc[6]/str[@name='id'][.='a']");

      // now switch the order:
      booster.setTopQueryResults(reader, query, false, new String[] {"a", "x"}, null);
      assertQ(
          req(baseParams),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='a']",
          "//result/doc[2]/str[@name='id'][.='x']",
          "//result/doc[3]/str[@name='id'][.='c']",
          "//result/doc[4]/str[@name='id'][.='b']");

      // Try normal sort by 'id'
      // default 'forceBoost' should be false
      assertFalse(booster.forceElevation);
      assertQ(
          req(baseParams, "sort", "id asc"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='a']",
          "//result/doc[2]/str[@name='id'][.='b']",
          "//result/doc[3]/str[@name='id'][.='c']",
          "//result/doc[4]/str[@name='id'][.='x']");

      assertQ(
          "useConfiguredElevatedOrder=false",
          req(baseParams, "sort", "str_s1 asc,id desc", "useConfiguredElevatedOrder", "false"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='x']", // group1
          "//result/doc[2]/str[@name='id'][.='a']", // group1
          "//result/doc[3]/str[@name='id'][.='c']",
          "//result/doc[4]/str[@name='id'][.='b']");

      booster.forceElevation = true;
      assertQ(
          req(baseParams, "sort", "id asc"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='a']",
          "//result/doc[2]/str[@name='id'][.='x']",
          "//result/doc[3]/str[@name='id'][.='b']",
          "//result/doc[4]/str[@name='id'][.='c']");

      booster.forceElevation = true;
      assertQ(
          "useConfiguredElevatedOrder=false and forceElevation",
          req(baseParams, "sort", "id desc", "useConfiguredElevatedOrder", "false"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='x']", // force elevated
          "//result/doc[2]/str[@name='id'][.='a']", // force elevated
          "//result/doc[3]/str[@name='id'][.='c']",
          "//result/doc[4]/str[@name='id'][.='b']");

      // Test exclusive (not to be confused with exclusion)
      booster.setTopQueryResults(reader, query, false, new String[] {"x", "a"}, new String[] {});
      assertQ(
          req(baseParams, "exclusive", "true"),
          "//*[@numFound='2']",
          "//result/doc[1]/str[@name='id'][.='x']",
          "//result/doc[2]/str[@name='id'][.='a']");

      // Test exclusion
      booster.setTopQueryResults(reader, query, false, new String[] {"x"}, new String[] {"a"});
      assertQ(
          req(baseParams),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='x']",
          "//result/doc[2]/str[@name='id'][.='c']",
          "//result/doc[3]/str[@name='id'][.='b']");

      // Test setting ids and excludes from http parameters

      booster.clearElevationProviderCache();
      assertQ(
          "All five should make it",
          req(baseParams, "elevateIds", "x,y,z", "excludeIds", "b"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][.='x']",
          "//result/doc[2]/str[@name='id'][.='y']",
          "//result/doc[3]/str[@name='id'][.='z']",
          "//result/doc[4]/str[@name='id'][.='c']",
          "//result/doc[5]/str[@name='id'][.='a']");

      assertQ(
          "All four should make it",
          req(baseParams, "elevateIds", "x,z,y", "excludeIds", "b,c"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='x']",
          "//result/doc[2]/str[@name='id'][.='z']",
          "//result/doc[3]/str[@name='id'][.='y']",
          "//result/doc[4]/str[@name='id'][.='a']");

    } finally {
      delete();
    }
  }

  // write an elevation config file to boost some docs
  private void writeElevationConfigFile(File file, String query, String... ids) throws Exception {
    try (PrintWriter out =
        new PrintWriter(Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8))) {
      out.println("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
      out.println("<elevate>");
      out.println("<query text=\"" + query + "\">");
      for (String id : ids) {
        out.println(" <doc id=\"" + id + "\"/>");
      }
      out.println("</query>");
      out.println("</elevate>");
      out.flush();
    }

    if (log.isInfoEnabled()) {
      log.info("OUT: {}", file.getAbsolutePath());
    }
  }

  @Test
  public void testElevationReloading() throws Exception {
    // need a mutable solr home.  Copying all collection1 is a lot but this is only one test.
    final Path solrHome = createTempDir();
    copyMinConf(solrHome.resolve("collection1").toFile(), null, "solrconfig-elevate.xml");

    File configFile =
        solrHome.resolve("collection1").resolve("conf").resolve("elevate.xml").toFile();
    writeElevationConfigFile(configFile, "aaa", "A");

    initCore("solrconfig.xml", "schema.xml", solrHome.toString());

    try {

      QueryElevationComponent comp =
          (QueryElevationComponent) h.getCore().getSearchComponent("elevate");
      NamedList<String> args = new NamedList<>();
      args.add(QueryElevationComponent.CONFIG_FILE, configFile.getName());
      comp.init(args);
      comp.inform(h.getCore());

      QueryElevationComponent.ElevationProvider elevationProvider;

      try (SolrQueryRequest req = req()) {
        elevationProvider =
            comp.getElevationProvider(req.getSearcher().getIndexReader(), req.getCore());
        assertTrue(
            elevationProvider.getElevationForQuery("aaa").elevatedIds.contains(new BytesRef("A")));
        assertNull(elevationProvider.getElevationForQuery("bbb"));
      }

      // now change the file
      writeElevationConfigFile(configFile, "bbb", "B");

      // With no index change, we get the same index reader, so the elevationProviderCache returns
      // the previous ElevationProvider without the change.
      try (SolrQueryRequest req = req()) {
        elevationProvider =
            comp.getElevationProvider(req.getSearcher().getIndexReader(), req.getCore());
        assertTrue(
            elevationProvider.getElevationForQuery("aaa").elevatedIds.contains(new BytesRef("A")));
        assertNull(elevationProvider.getElevationForQuery("bbb"));
      }

      // Index a new doc to get a new index reader.
      assertU(adoc("id", "10000"));
      assertU(commit());

      // Check that we effectively reload a new ElevationProvider for a different index reader (so
      // two entries in elevationProviderCache).
      try (SolrQueryRequest req = req()) {
        elevationProvider =
            comp.getElevationProvider(req.getSearcher().getIndexReader(), req.getCore());
        assertNull(elevationProvider.getElevationForQuery("aaa"));
        assertTrue(
            elevationProvider.getElevationForQuery("bbb").elevatedIds.contains(new BytesRef("B")));
      }

      // Now change the config file again.
      writeElevationConfigFile(configFile, "ccc", "C");

      // Without index change, but calling a different method that clears the
      // elevationProviderCache, so we should load a new ElevationProvider.
      int elevationRuleNumber = comp.loadElevationConfiguration(h.getCore());
      assertEquals(1, elevationRuleNumber);
      try (SolrQueryRequest req = req()) {
        elevationProvider =
            comp.getElevationProvider(req.getSearcher().getIndexReader(), req.getCore());
        assertNull(elevationProvider.getElevationForQuery("aaa"));
        assertNull(elevationProvider.getElevationForQuery("bbb"));
        assertTrue(
            elevationProvider.getElevationForQuery("ccc").elevatedIds.contains(new BytesRef("C")));
      }
    } finally {
      delete();
    }
  }

  @Test
  public void testWithLocalParam() throws Exception {
    try {
      init("schema11.xml");
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "7", "text", "AAAA", "str_s", "a"));
      assertU(commit());

      assertQ(
          "",
          req(
              CommonParams.Q,
              "AAAA",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='7']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");
      assertQ(
          "",
          req(
              CommonParams.Q,
              "{!q.op=AND}AAAA",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='7']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");
      assertQ(
          "",
          req(
              CommonParams.Q,
              "{!q.op=AND v='AAAA'}",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.='7']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']");
    } finally {
      delete();
    }
  }

  @Test
  public void testQuerySubsetMatching() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "1", "title", "XXXX", "str_s1", "a"));
      assertU(adoc("id", "2", "title", "YYYY", "str_s1", "b"));
      assertU(adoc("id", "3", "title", "ZZZZ", "str_s1", "c"));

      assertU(adoc("id", "4", "title", "XXXX XXXX", "str_s1", "x"));
      assertU(adoc("id", "5", "title", "YYYY YYYY", "str_s1", "y"));
      assertU(adoc("id", "6", "title", "XXXX XXXX", "str_s1", "z"));
      assertU(adoc("id", "7", "title", "AAAA", "str_s1", "a"));

      assertU(adoc("id", "10", "title", "RR", "str_s1", "r"));
      assertU(adoc("id", "11", "title", "SS", "str_s1", "r"));
      assertU(adoc("id", "12", "title", "TT", "str_s1", "r"));
      assertU(adoc("id", "13", "title", "UU", "str_s1", "r"));
      assertU(adoc("id", "14", "title", "VV", "str_s1", "r"));
      assertU(commit());

      // Exact matching.
      assertQ(
          "",
          req(
              CommonParams.Q,
              "XXXX",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='4']",
          "//result/doc[3]/str[@name='id'][.='6']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='false']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']");

      // Exact matching.
      assertQ(
          "",
          req(
              CommonParams.Q,
              "QQQQ EE",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='0']");

      // Subset matching.
      assertQ(
          "",
          req(
              CommonParams.Q,
              "BB DD CC VV",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='10']",
          "//result/doc[2]/str[@name='id'][.='12']",
          "//result/doc[3]/str[@name='id'][.='11']",
          "//result/doc[4]/str[@name='id'][.='14']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']",
          "//result/doc[4]/bool[@name='[elevated]'][.='false']");

      // Subset + exact matching.
      assertQ(
          "",
          req(
              CommonParams.Q,
              "BB CC",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='13']",
          "//result/doc[2]/str[@name='id'][.='10']",
          "//result/doc[3]/str[@name='id'][.='12']",
          "//result/doc[4]/str[@name='id'][.='11']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']",
          "//result/doc[4]/bool[@name='[elevated]'][.='true']");

      // Subset matching.
      assertQ(
          "",
          req(
              CommonParams.Q,
              "AA BB DD CC AA",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='10']",
          "//result/doc[2]/str[@name='id'][.='12']",
          "//result/doc[3]/str[@name='id'][.='11']",
          "//result/doc[4]/str[@name='id'][.='14']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']",
          "//result/doc[4]/bool[@name='[elevated]'][.='true']");

      // Subset matching.
      assertQ(
          "",
          req(
              CommonParams.Q,
              "AA RR BB DD AA",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='12']",
          "//result/doc[2]/str[@name='id'][.='14']",
          "//result/doc[3]/str[@name='id'][.='10']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']");

      // Subset matching.
      assertQ(
          "",
          req(
              CommonParams.Q,
              "AA BB EE",
              CommonParams.QT,
              "/elevate",
              CommonParams.FL,
              "id, score, [elevated]"),
          "//*[@numFound='0']");
    } finally {
      delete();
    }
  }

  @Test
  public void testElevatedIds() throws Exception {
    try (QueryElevationComponent comp = new QueryElevationComponent()) {
      init("schema12.xml");
      SolrCore core = h.getCore();

      NamedList<String> args = new NamedList<>();
      args.add(QueryElevationComponent.FIELD_TYPE, "text");
      args.add(QueryElevationComponent.CONFIG_FILE, "elevate.xml");

      comp.init(args);
      comp.inform(core);

      SolrQueryRequest req = req();
      IndexReader reader = req.getSearcher().getIndexReader();
      QueryElevationComponent.ElevationProvider elevationProvider =
          comp.getElevationProvider(reader, core);
      req.close();

      assertEquals(toIdSet("1"), elevationProvider.getElevationForQuery("xxxx").elevatedIds);
      assertEquals(
          toIdSet("10", "11", "12"),
          elevationProvider.getElevationForQuery("bb DD CC vv").elevatedIds);
      assertEquals(
          toIdSet("10", "11", "12", "13"),
          elevationProvider.getElevationForQuery("BB Cc").elevatedIds);
      assertEquals(
          toIdSet("10", "11", "12", "14"),
          elevationProvider.getElevationForQuery("aa bb dd cc aa").elevatedIds);
    } finally {
      delete();
    }
  }

  @Test
  public void testOnlyDocsInSearchResultsWillBeElevated() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "1", "title", "XXXX", "str_s1", "a"));
      assertU(adoc("id", "2", "title", "YYYY", "str_s1", "b"));
      assertU(adoc("id", "3", "title", "ZZZZ", "str_s1", "c"));

      assertU(adoc("id", "4", "title", "XXXX XXXX", "str_s1", "x"));
      assertU(adoc("id", "5", "title", "YYYY YYYY", "str_s1", "y"));
      assertU(adoc("id", "6", "title", "XXXX XXXX", "str_s1", "z"));
      assertU(adoc("id", "7", "title", "AAAA", "str_s1", "a"));

      assertU(commit());

      // default behaviour
      assertQ(
          "",
          req(
              CommonParams.Q, "YYYY",
              CommonParams.QT, "/elevate",
              QueryElevationParams.ELEVATE_ONLY_DOCS_MATCHING_QUERY, "false",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='5']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']");

      // only docs that matches q
      assertQ(
          "",
          req(
              CommonParams.Q, "YYYY",
              CommonParams.QT, "/elevate",
              QueryElevationParams.ELEVATE_ONLY_DOCS_MATCHING_QUERY, "true",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='2']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[2]/str[@name='id'][.='5']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='false']");

    } finally {
      delete();
    }
  }

  @Test
  public void testOnlyRepresentativeIsVisibleWhenCollapsing() throws Exception {
    try {
      init("schema12.xml");
      assertU(adoc("id", "1", "title", "ZZZZ", "str_s1", "a"));
      assertU(adoc("id", "2", "title", "ZZZZ", "str_s1", "b"));
      assertU(adoc("id", "3", "title", "ZZZZ ZZZZ", "str_s1", "a"));
      assertU(adoc("id", "4", "title", "ZZZZ ZZZZ", "str_s1", "c"));

      assertU(commit());

      // default behaviour - all elevated docs are visible
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CollapsingQParserPlugin.COLLECT_ELEVATED_DOCS_WHEN_COLLAPSING, "true",
              CommonParams.FQ, "{!collapse field=str_s1 sort='score desc'}",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='3']",
          "//result/doc[4]/str[@name='id'][.='4']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='true']",
          "//result/doc[4]/bool[@name='[elevated]'][.='false']");

      // only representative elevated doc visible
      assertQ(
          "",
          req(
              CommonParams.Q, "ZZZZ",
              CommonParams.QT, "/elevate",
              CollapsingQParserPlugin.COLLECT_ELEVATED_DOCS_WHEN_COLLAPSING, "false",
              CommonParams.FQ, "{!collapse field=str_s1 sort='score desc'}",
              CommonParams.FL, "id, score, [elevated]"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='2']",
          "//result/doc[2]/str[@name='id'][.='3']",
          "//result/doc[3]/str[@name='id'][.='4']",
          "//result/doc[1]/bool[@name='[elevated]'][.='true']",
          "//result/doc[2]/bool[@name='[elevated]'][.='true']",
          "//result/doc[3]/bool[@name='[elevated]'][.='false']");

    } finally {
      delete();
    }
  }

  @Test
  public void testCursor() throws Exception {
    try {
      init("schema12.xml");

      assertU(adoc("id", "a", "title", "ipod trash trash", "str_s1", "group1"));
      assertU(adoc("id", "b", "title", "ipod ipod  trash", "str_s1", "group2"));
      assertU(adoc("id", "c", "title", "ipod ipod  ipod ", "str_s1", "group2"));

      assertU(adoc("id", "x", "title", "boosted", "str_s1", "group1"));
      assertU(adoc("id", "y", "title", "boosted boosted", "str_s1", "group2"));
      assertU(adoc("id", "z", "title", "boosted boosted boosted", "str_s1", "group2"));
      assertU(commit());

      final SolrParams baseParams =
          params(
              "qt", "/elevate",
              "q", "title:ipod",
              "sort", "score desc, id asc",
              "fl", "id",
              "elevateIds", "x,y,z",
              "excludeIds", "b");

      // sanity check everything returned w/these elevation options...
      assertJQ(
          req(baseParams),
          "/response/numFound==5",
          "/response/start==0",
          "/response/docs==[{'id':'x'},{'id':'y'},{'id':'z'},{'id':'c'},{'id':'a'}]");
      // same query using CURSOR_MARK_START should produce a 'next' cursor...
      assertCursorJQ(
          req(baseParams, CURSOR_MARK_PARAM, CURSOR_MARK_START),
          "/response/numFound==5",
          "/response/start==0",
          "/response/docs==[{'id':'x'},{'id':'y'},{'id':'z'},{'id':'c'},{'id':'a'}]");

      // use a cursor w/rows < 5, then fetch next cursor...
      String nextCursor = null;
      nextCursor =
          assertCursorJQ(
              req(baseParams, CURSOR_MARK_PARAM, CURSOR_MARK_START, "rows", "2"),
              "/response/numFound==5",
              "/response/start==0",
              "/response/docs==[{'id':'x'},{'id':'y'}]");
      nextCursor =
          assertCursorJQ(
              req(baseParams, CURSOR_MARK_PARAM, nextCursor, "rows", "2"),
              "/response/numFound==5",
              "/response/start==0",
              "/response/docs==[{'id':'z'},{'id':'c'}]");
      nextCursor =
          assertCursorJQ(
              req(baseParams, CURSOR_MARK_PARAM, nextCursor, "rows", "2"),
              "/response/numFound==5",
              "/response/start==0",
              "/response/docs==[{'id':'a'}]");
      final String lastCursor = nextCursor;
      nextCursor =
          assertCursorJQ(
              req(baseParams, CURSOR_MARK_PARAM, nextCursor, "rows", "2"),
              "/response/numFound==5",
              "/response/start==0",
              "/response/docs==[]");
      assertEquals(lastCursor, nextCursor);

    } finally {
      delete();
    }
  }

  private static Set<BytesRef> toIdSet(String... ids) {
    return Arrays.stream(ids).map(BytesRef::new).collect(Collectors.toSet());
  }

  /**
   * Asserts that the query matches the specified JSON patterns and then returns the {@link
   * CursorMarkParams#CURSOR_MARK_NEXT} value from the response
   *
   * @see #assertJQ
   */
  private static String assertCursorJQ(SolrQueryRequest req, String... tests) throws Exception {
    String json = assertJQ(req, tests);
    Map<?, ?> rsp = (Map<?, ?>) fromJSONString(json);
    assertTrue(
        "response doesn't contain " + CURSOR_MARK_NEXT + ": " + json,
        rsp.containsKey(CURSOR_MARK_NEXT));
    String next = (String) rsp.get(CURSOR_MARK_NEXT);
    assertNotNull(CURSOR_MARK_NEXT + " is null", next);
    return next;
  }
}

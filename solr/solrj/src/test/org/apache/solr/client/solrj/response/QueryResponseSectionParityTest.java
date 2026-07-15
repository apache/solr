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
package org.apache.solr.client.solrj.response;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ResponseNormalizer;
import org.junit.Test;

/**
 * Each test feeds a JSON (json.nl=map) response for one QueryResponse section through the
 * normalizer and asserts the typed accessor works. Sections with a numeric cast (grouping, facets,
 * spellcheck) also guard the Number widening; the rest guard the structural Map -&gt; NamedList /
 * SolrDocumentList reconstruction the section relies on.
 */
public class QueryResponseSectionParityTest extends SolrTestCase {

  private static QueryResponse parse(String json) throws Exception {
    NamedList<Object> parsed =
        new JsonMapResponseParser()
            .processResponse(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), "UTF-8");
    QueryResponse r = new QueryResponse();
    r.setResponse(ResponseNormalizer.normalize(parsed));
    return r;
  }

  private static final String HEADER = "\"responseHeader\":{\"status\":0,\"QTime\":1},";

  /** pivot facets: count is an Integer cast (QueryResponse readPivots). */
  @Test
  public void testPivotFacets() throws Exception {
    String json =
        "{"
            + HEADER
            + "\"facet_counts\":{\"facet_queries\":{},\"facet_fields\":{},"
            + "\"facet_pivot\":{\"cat\":[{\"field\":\"cat\",\"value\":\"electronics\",\"count\":3}]}}}";
    QueryResponse r = parse(json);
    assertNotNull("facetPivot", r.getFacetPivot());
    assertEquals(3, r.getFacetPivot().get("cat").get(0).getCount());
  }

  /** grouping: matches / ngroups are Integer casts (QueryResponse extractGroupedInfo). */
  @Test
  public void testGrouping() throws Exception {
    String json =
        "{"
            + HEADER
            + "\"grouped\":{\"cat\":{\"matches\":3,\"ngroups\":2,\"groups\":["
            + "{\"groupValue\":\"a\",\"doclist\":{\"numFound\":2,\"start\":0,\"docs\":[{\"id\":\"1\"}]}},"
            + "{\"groupValue\":\"b\",\"doclist\":{\"numFound\":1,\"start\":0,\"docs\":[{\"id\":\"2\"}]}}"
            + "]}}}";
    QueryResponse r = parse(json);
    GroupResponse gr = r.getGroupResponse();
    assertNotNull("groupResponse", gr);
    assertEquals(3, gr.getValues().get(0).getMatches());
    assertEquals(Integer.valueOf(2), gr.getValues().get(0).getNGroups());
  }

  /** interval facets: count is an Integer cast (QueryResponse extractFacetInfo). */
  @Test
  public void testIntervalFacets() throws Exception {
    String json =
        "{"
            + HEADER
            + "\"facet_counts\":{\"facet_queries\":{},\"facet_fields\":{},"
            + "\"facet_intervals\":{\"price\":{\"[0,10]\":5,\"[11,100]\":3}}}}";
    QueryResponse r = parse(json);
    assertNotNull("intervalFacets", r.getIntervalFacets());
    assertEquals(2, r.getIntervalFacets().get(0).getIntervals().size());
    assertEquals(5, r.getIntervalFacets().get(0).getIntervals().get(0).getCount());
  }

  /** field stats: count/missing (Long) and sumOfSquares/stddev (Double) casts (FieldStatsInfo). */
  @Test
  public void testFieldStats() throws Exception {
    String json =
        "{"
            + HEADER
            + "\"stats\":{\"stats_fields\":{\"price\":{"
            + "\"min\":9.0,\"max\":12.0,\"count\":2,\"missing\":0,"
            + "\"sumOfSquares\":225.0,\"stddev\":1.5,\"countDistinct\":2,\"cardinality\":2}}}}";
    QueryResponse r = parse(json);
    assertNotNull("fieldStatsInfo", r.getFieldStatsInfo());
    FieldStatsInfo price = r.getFieldStatsInfo().get("price");
    assertNotNull("price stats", price);
    assertEquals(Long.valueOf(2), price.getCount());
    assertEquals(Long.valueOf(0), price.getMissing());
    assertEquals(Double.valueOf(1.5), price.getStddev());
    assertEquals(Long.valueOf(2), price.getCardinality());
  }

  /** spellcheck: numFound / startOffset / origFreq are Integer casts (SpellCheckResponse). */
  @Test
  public void testSpellCheck() throws Exception {
    String json =
        "{"
            + HEADER
            + "\"spellcheck\":{\"suggestions\":{"
            + "\"helo\":{\"numFound\":1,\"startOffset\":0,\"endOffset\":4,\"origFreq\":0,"
            + "\"suggestion\":[{\"word\":\"hello\",\"freq\":5}]}}}}";
    QueryResponse r = parse(json);
    SpellCheckResponse sc = r.getSpellCheckResponse();
    assertNotNull("spellcheck", sc);
    SpellCheckResponse.Suggestion s = sc.getSuggestion("helo");
    assertNotNull("suggestion", s);
    assertEquals(1, s.getNumFound());
    assertEquals(0, s.getStartOffset());
    assertEquals(Integer.valueOf(5), s.getAlternativeFrequencies().get(0));
  }

  /** highlighting: no numeric cast, but exercises Map->NamedList reconstruction over JSON. */
  @Test
  public void testHighlighting() throws Exception {
    String json = "{" + HEADER + "\"highlighting\":{\"1\":{\"name\":[\"<em>foo</em>\"]}}}";
    QueryResponse r = parse(json);
    assertNotNull("highlighting", r.getHighlighting());
    assertEquals("<em>foo</em>", r.getHighlighting().get("1").get("name").get(0));
  }

  /** terms: df/ttf are read via Number, and the section is a nested NamedList over JSON. */
  @Test
  public void testTerms() throws Exception {
    String json = "{" + HEADER + "\"terms\":{\"cat\":{\"electronics\":3,\"books\":1}}}";
    QueryResponse r = parse(json);
    assertNotNull("termsResponse", r.getTermsResponse());
    assertEquals(2, r.getTermsResponse().getTerms("cat").size());
    assertEquals(3L, r.getTermsResponse().getTerms("cat").get(0).getFrequency());
  }

  /**
   * moreLikeThis: each value is a {numFound,docs} object -> must reconstruct as SolrDocumentList.
   */
  @Test
  public void testMoreLikeThis() throws Exception {
    String json =
        "{"
            + HEADER
            + "\"moreLikeThis\":{\"1\":{\"numFound\":1,\"start\":0,\"docs\":[{\"id\":\"2\"}]}}}";
    QueryResponse r = parse(json);
    assertNotNull("moreLikeThis", r.getMoreLikeThis());
    assertEquals(1, r.getMoreLikeThis().get("1").getNumFound());
    assertEquals("2", r.getMoreLikeThis().get("1").get(0).getFirstValue("id"));
  }
}

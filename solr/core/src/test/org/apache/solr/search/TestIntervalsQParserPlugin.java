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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for {@link IntervalsQParserPlugin}. */
public class TestIntervalsQParserPlugin extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema11.xml");
  }

  @Test
  public void testIntervalsMissingQueryThrows() throws Exception {
    assertU(adoc("id", "1", "v_t", "hello world"));
    assertU(commit());

    // Without a rule in the intervals json the parser must explain that a rule key is required
    assertQEx(
        "intervals qparser without a rule should throw BAD_REQUEST",
        "Each rule object must contain exactly one rule key",
        req("json", "{query:{intervals:{use_field:v_t}}, fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsMissingJsonBodyThrows() throws Exception {
    assertU(adoc("id", "2", "v_t", "hello world"));
    assertU(commit());

    // With a $name reference but no JSON request body at all
    assertQEx(
        "intervals qparser without a JSON body should throw BAD_REQUEST",
        "No JSON request body found",
        req("q", "{!intervals df=v_t}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsMatchRuleMatchesDocument() throws Exception {
    assertU(adoc("id", "10", "v_t", "foo bar"));
    assertU(adoc("id", "11", "v_t", "baz qux"));
    assertU(commit());

    // field specified via use_field in the intervals JSON body
    assertJJQ(
        "{ query:{intervals:{use_field:v_t, match:{query:foo}}}," + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'10'}]}");
  }

  @Test
  public void testIntervalsAllOfAnyOfNamedQuery() throws Exception {
    assertU(adoc("id", "30", "title_t", "alpha beta gamma delta"));
    assertU(adoc("id", "31", "title_t", "alpha beta epsilon delta"));
    assertU(adoc("id", "32", "title_t", "alpha zeta gamma delta"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:title_t,"
            + "all_of:{ordered:true,intervals:["
            + "{match:{query:'alpha beta', max_gaps:0, ordered:true}},"
            + "{any_of:{intervals:["
            + "{match:{query:'gamma delta', max_gaps:0, ordered:true}},"
            + "{match:{query:'epsilon delta', max_gaps:0, ordered:true}}"
            + "]}}"
            + "]}}},"
            + "fields:id}",
        "/response=={'numFound':2,'start':0,'numFoundExact':true}");
  }

  @Test
  public void testIntervalsNoMatchingRule() throws Exception {
    assertU(adoc("id", "20", "v_t", "hello world"));
    assertU(commit());

    // Match rule text not present in any document; field via use_field
    assertJJQ(
        "{ query:{intervals:{use_field:v_t, match:{query:zzznomatch}}}," + "fields:id}",
        "/response=={'numFound':0,'start':0,'numFoundExact':true}");
  }

  @Test
  public void testIntervalsTermRule() throws Exception {
    assertU(adoc("id", "40", "v_ws", "trm_apple trm_banana"));
    assertU(adoc("id", "41", "v_ws", "trm_banana trm_cherry"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws, term:{value:trm_apple}}}," + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'40'}]}");
  }

  @Test
  public void testIntervalsPhraseRuleWithTerms() throws Exception {
    assertU(adoc("id", "50", "v_ws", "phrA_quick phrA_brown phrA_fox"));
    assertU(adoc("id", "51", "v_ws", "phrA_quick phrA_fox phrA_brown"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "phrase:{terms:[phrA_quick,phrA_brown,phrA_fox]}}},"
            + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'50'}]}");
  }

  @Test
  public void testIntervalsPhraseRuleWithIntervals() throws Exception {
    assertU(adoc("id", "52", "v_ws", "phrB_quick phrB_brown phrB_fox"));
    assertU(adoc("id", "53", "v_ws", "phrB_quick phrB_fox phrB_brown"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "phrase:{intervals:"
            + "[{term:{value:phrB_quick}},{term:{value:phrB_brown}},{term:{value:phrB_fox}}]}}},"
            + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'52'}]}");

    assertQEx(
        "phrase rule should reject specifying both 'terms' and 'intervals' with BAD_REQUEST",
        "cannot specify both 'terms' and 'intervals'",
        req(
            "json",
            "{query:{intervals:{use_field:v_ws,"
                + "phrase:{terms:[phrB_quick,phrB_brown],intervals:"
                + "[{term:{value:phrB_quick}},{term:{value:phrB_brown}}]}}},"
                + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsRegexpRule() throws Exception {
    assertU(adoc("id", "60", "v_ws", "rx_cat"));
    assertU(adoc("id", "61", "v_ws", "rx_car"));
    assertU(adoc("id", "62", "v_ws", "rx_dog"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws, regexp:{pattern:'rx_ca.*'}}}," + "fields:id}",
        "/response=={'numFound':2,'start':0,'numFoundExact':true}");

    assertQEx(
        "regexp rule should reject a negative max_expansions with BAD_REQUEST",
        "max_expansions",
        req(
            "json",
            "{query:{intervals:{use_field:v_ws, regexp:{pattern:'rx_ca.*',max_expansions:-1}}},"
                + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsRangeRule() throws Exception {
    assertU(adoc("id", "70", "v_ws", "rng_aaaa"));
    assertU(adoc("id", "71", "v_ws", "rng_bbbb"));
    assertU(adoc("id", "72", "v_ws", "rng_cccc"));
    assertU(adoc("id", "73", "v_ws", "rng_dddd"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "range:{lower_term:rng_bbbb,upper_term:rng_cccc,"
            + "include_lower:true,include_upper:true}}},"
            + "fields:id}",
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'71'},{'id':'72'}]}");

    assertQEx(
        "range rule should reject a negative max_expansions with BAD_REQUEST",
        "max_expansions",
        req(
            "json",
            "{query:{intervals:{use_field:v_ws,"
                + "range:{lower_term:rng_bbbb,upper_term:rng_cccc,"
                + "include_lower:true,include_upper:true,max_expansions:-1}}},"
                + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsFuzzyRule() throws Exception {
    assertU(adoc("id", "80", "v_ws", "fzz_cat"));
    assertU(adoc("id", "81", "v_ws", "fzz_car"));
    assertU(adoc("id", "82", "v_ws", "fzz_dog"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws, fuzzy:{term:fzz_cat,fuzziness:'1'}}}," + "fields:id}",
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'80'},{'id':'81'}]}");

    assertQEx(
        "fuzzy rule should reject a negative prefix_length with BAD_REQUEST",
        "prefix_length",
        req(
            "json",
            "{query:{intervals:{use_field:v_ws, fuzzy:{term:fzz_cat,prefix_length:-1}}},"
                + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsMaxWidthRule() throws Exception {
    assertU(adoc("id", "80", "v_ws", "mwd_alpha mwd_beta mwd_gamma"));
    assertU(adoc("id", "81", "v_ws", "mwd_alpha mwd_zeta mwd_gamma"));
    assertU(commit());

    // ordered phrase "mwd_alpha mwd_beta" has width 2 (positions 0..1); doc 81 has no mwd_beta
    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "max_width:{width:2,source:"
            + "{all_of:{ordered:true,intervals:[{term:{value:mwd_alpha}},{term:{value:mwd_beta}}]}}}}},"
            + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'80'}]}");
  }

  @Test
  public void testIntervalsExtendRule() throws Exception {
    assertU(adoc("id", "90", "v_ws", "ext_one ext_two ext_three ext_four ext_five"));
    assertU(adoc("id", "91", "v_ws", "ext_one ext_five"));
    assertU(commit());

    // extend ext_three by before=2 and after=2; doc 90 has ext_three, doc 91 does not
    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "extend:{source:{term:{value:ext_three}},before:2,after:2}}},"
            + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'90'}]}");
  }

  @Test
  public void testIntervalsUnorderedNoOverlapsRule() throws Exception {
    assertU(adoc("id", "100", "v_ws", "uno_foo uno_bar"));
    assertU(adoc("id", "101", "v_ws", "uno_bar uno_foo"));
    assertU(adoc("id", "102", "v_ws", "uno_baz uno_qux"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "unordered_no_overlaps:{intervals:"
            + "[{term:{value:uno_foo}},{term:{value:uno_bar}}]}}},"
            + "fields:id}",
        "/response=={'numFound':2,'start':0,'numFoundExact':true}");
  }

  @Test
  public void testIntervalsWithinRule() throws Exception {
    assertU(adoc("id", "110", "v_ws", "wth_alpha wth_beta wth_gamma"));
    assertU(adoc("id", "111", "v_ws", "wth_alpha wth_zeta wth_eps wth_gamma"));
    assertU(commit());

    // "wth_alpha" within 1 position of "wth_beta": doc 110 matches (adjacent), doc 111 does not
    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "within:{source:{term:{value:wth_alpha}},"
            + "positions:1,reference:{term:{value:wth_beta}}}}},"
            + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'110'}]}");
  }

  @Test
  public void testIntervalsNotWithinRule() throws Exception {
    assertU(adoc("id", "120", "v_ws", "nwt_alpha nwt_zeta nwt_eps nwt_gamma"));
    assertU(adoc("id", "121", "v_ws", "nwt_alpha nwt_beta nwt_gamma"));
    assertU(commit());

    // "nwt_alpha" NOT within 1 position of "nwt_beta": doc 121 has them adjacent (excluded),
    // doc 120 has no nwt_beta so nwt_alpha qualifies
    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "not_within:{source:{term:{value:nwt_alpha}},"
            + "positions:1,reference:{term:{value:nwt_beta}}}}},"
            + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'120'}]}");
  }

  @Test
  public void testIntervalsAtLeastRule() throws Exception {
    assertU(adoc("id", "130", "v_ws", "atl_alpha atl_beta atl_gamma"));
    assertU(adoc("id", "131", "v_ws", "atl_alpha atl_gamma"));
    assertU(adoc("id", "132", "v_ws", "atl_delta atl_epsilon"));
    assertU(commit());

    // at_least 2 of [atl_alpha, atl_beta, atl_gamma]: doc 130 has all 3, doc 131 has 2, doc 132 has
    // none
    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "at_least:{min_should_match:2,intervals:"
            + "[{term:{value:atl_alpha}},{term:{value:atl_beta}},{term:{value:atl_gamma}}]}}},"
            + "fields:id}",
        "/response=={'numFound':2,'start':0,'numFoundExact':true}");
  }

  @Test
  public void testIntervalsNoIntervalsRule() throws Exception {
    assertU(adoc("id", "140", "v_ws", "nio_anything"));
    assertU(commit());

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws, no_intervals:{reason:testing}}}," + "fields:id}",
        "/response=={'numFound':0,'start':0,'numFoundExact':true}");
  }

  @Test
  public void testIntervalsDfFallbackFromQueryParam() throws Exception {
    assertU(adoc("id", "150", "v_ws", "dfp_alpha dfp_beta"));
    assertU(adoc("id", "151", "v_ws", "dfp_gamma dfp_delta"));
    assertU(commit());

    // df supplied as a regular query param (not a local param) should be used as the field when
    // use_field is absent in the JSON body
    assertJJQ(
        "{ query:{intervals:{term:{value:dfp_alpha}}}," + "params:{df:v_ws}," + "fields:id}",
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'150'}]}");
  }

  @Test
  public void testIntervalsLegacyFieldInJsonQueryThrows() throws Exception {
    assertU(adoc("id", "160", "v_ws", "bkc_alpha bkc_beta"));
    assertU(commit());

    // Old {field: rule_object} format is no longer supported: even with a valid use_field, the
    // field name is mistaken for an (unsupported) rule name since rule objects must be
    // {rule_name: {...}}, not {field_name: {...}}.
    assertQEx(
        "legacy {field: rule} format should throw BAD_REQUEST for an unrecognized rule name",
        "Unsupported intervals rule: v_ws",
        req(
            "json",
            "{query:{intervals:{use_field:v_ws, v_ws:{term:{value:bkc_alpha}}}}," + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsMatchRuleNonexistentDfFieldThrows() throws Exception {
    // use_field names a field that doesn't exist in the schema; the match rule resolves its
    // analyzer from this field
    assertQEx(
        "match rule with a nonexistent use_field should throw BAD_REQUEST",
        "undefined field",
        req(
            "json",
            "{query:{intervals:{use_field:no_such_field, match:{query:foo}}}," + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsMatchRuleNonexistentUseFieldThrows() throws Exception {
    // use_field is valid, but the match query's use_field overrides the resolver field
    assertQEx(
        "match rule with a nonexistent use_field should throw BAD_REQUEST",
        "undefined field",
        req(
            "json",
            "{query:{intervals:{use_field:v_t,"
                + " match:{query:foo,use_field:no_such_field}}},"
                + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsPrefixRuleNonexistentDfFieldThrows() throws Exception {
    // prefix/wildcard/fuzzy resolve their multi-term analyzer from the field the same way
    assertQEx(
        "prefix rule with a nonexistent use_field should throw BAD_REQUEST",
        "undefined field",
        req(
            "json",
            "{query:{intervals:{use_field:no_such_field, prefix:{prefix:foo}}}," + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsPrefixRuleNonexistentUseFieldThrows() throws Exception {
    assertQEx(
        "prefix rule with a nonexistent use_field should throw BAD_REQUEST",
        "undefined field",
        req(
            "json",
            "{query:{intervals:{use_field:v_ws,"
                + " prefix:{prefix:foo,use_field:no_such_field}}},"
                + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsUnknownAnalyzerFieldTypeThrows() throws Exception {
    // an explicit 'analyzer' value that doesn't match any field type name in the schema
    assertQEx(
        "match rule with an unknown analyzer field type should throw BAD_REQUEST",
        "Unknown analyzer",
        req(
            "json",
            "{query:{intervals:{use_field:v_t,"
                + " match:{query:foo,analyzer:no_such_field_type}}},"
                + "fields:id}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntervalsNestedAlternativeOutperformsXmlSpans() throws Exception {
    // v_ws (text_ws) is a plain whitespace-tokenized field with no stemming/synonym/word-delimiter
    // filters, so the literal terms used in the raw SpanTerm/term rules below match the indexed
    // tokens exactly.
    assertU(adoc("id", "170", "v_ws", "cmplorem cmpthe cmpdomain cmpis cmpipsum"));
    assertU(
        adoc("id", "171", "v_ws", "cmplorem cmpthe cmpdomain cmpname cmpsystem cmpis cmpipsum"));
    assertU(
        adoc("id", "172", "v_ws", "cmplorem cmpthe cmpdomain cmpblame cmpsystem cmpis cmpipsum"));
    assertU(commit());

    assertQ(
        "xmlparser SpanNear with nested SpanOr/SpanNear misses one nested match",
        req(
            "q",
            "{!xmlparser df=v_ws}"
                + "<SpanNear slop=\"0\" inOrder=\"true\" fieldName=\"v_ws\">"
                + "<SpanTerm fieldName=\"v_ws\">cmpthe</SpanTerm>"
                + "<SpanOr>"
                + "<SpanTerm fieldName=\"v_ws\">cmpdomain</SpanTerm>"
                + "<SpanNear slop=\"0\" inOrder=\"true\">"
                + "<SpanTerm fieldName=\"v_ws\">cmpdomain</SpanTerm>"
                + "<SpanTerm fieldName=\"v_ws\">cmpname</SpanTerm>"
                + "<SpanTerm fieldName=\"v_ws\">cmpsystem</SpanTerm>"
                + "</SpanNear>"
                + "</SpanOr>"
                + "<SpanTerm fieldName=\"v_ws\">cmpis</SpanTerm>"
                + "</SpanNear>"),
        "//result[@numFound='1']");

    assertJJQ(
        "{ query:{intervals:{use_field:v_ws,"
            + "all_of:{ordered:true,max_gaps:0,intervals:["
            + "{term:{value:cmpthe}},"
            + "{any_of:{intervals:["
            + "{term:{value:cmpdomain}},"
            + "{phrase:{terms:[cmpdomain,cmpname,cmpsystem]}}"
            + "]}}"
            + ",{term:{value:cmpis}}"
            + "]}}},"
            + "fields:id}",
        "/response=={'numFound':2,'start':0,'numFoundExact':true,"
            + "'docs':[{'id':'170'},{'id':'171'}]}");
  }
}

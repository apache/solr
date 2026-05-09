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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMatchedQueriesComponent extends SolrTestCaseJ4 {

  static final String HANDLER = "/matched-queries";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    // 4 fantasy books (ids 1–4), 2 of which are also childrens (ids 2–3)
    assertU(adoc("id", "1", "cat_s", "fantasy", "author_s1", "Lev Grossman"));
    assertU(
        adoc("id", "2", "cat_s", "fantasy", "cat_s", "childrens", "author_s1", "Robert Jordan"));
    assertU(
        adoc("id", "3", "cat_s", "fantasy", "cat_s", "childrens", "author_s1", "Robert Jordan"));
    assertU(adoc("id", "4", "cat_s", "fantasy", "author_s1", "N.K. Jemisin"));
    assertU(commit());
    // 3 scifi books (ids 5–7), in a separate segment
    assertU(adoc("id", "5", "cat_s", "scifi", "author_s1", "Ursula K. Le Guin"));
    assertU(adoc("id", "6", "cat_s", "scifi", "author_s1", "Ursula K. Le Guin"));
    assertU(adoc("id", "7", "cat_s", "scifi", "author_s1", "Isaac Asimov"));
    assertU(commit());
  }

  /** Component must be a no-op when the activation param is absent. */
  @Test
  public void testNotEnabledByDefault() throws Exception {
    assertJQ(
        req("qt", HANDLER, "q", "{!term _name=fantasy_cat f=cat_s}fantasy", "sort", "id asc"),
        "!/matched_queries_per_hit==null",
        "!/matched_queries_summary==null");
  }

  /** A single named term query: all 4 matching docs appear in per_hit and summary. */
  @Test
  public void testSingleNamedTermQuery() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q", "{!term _name=fantasy_cat f=cat_s}fantasy",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==4",
        "/matched_queries_per_hit/1/[0]=='fantasy_cat'",
        "/matched_queries_per_hit/2/[0]=='fantasy_cat'",
        "/matched_queries_per_hit/3/[0]=='fantasy_cat'",
        "/matched_queries_per_hit/4/[0]=='fantasy_cat'",
        "/matched_queries_summary/fantasy_cat/count==4",
        "/matched_queries_summary/fantasy_cat/docIds/[0]=='1'",
        "/matched_queries_summary/fantasy_cat/docIds/[3]=='4'");
  }

  /** The short alias {@code mq=true} must work identically to {@code matched_queries=true}. */
  @Test
  public void testShortParamAlias() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q", "{!term _name=fantasy_cat f=cat_s}fantasy",
            "mq", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==4",
        "/matched_queries_summary/fantasy_cat/count==4");
  }

  /**
   * Boolean OR of two named term queries: fantasy docs carry "fantasy_cat", scifi docs carry
   * "scifi_cat", no doc carries both.
   */
  @Test
  public void testTwoNamedQueriesOr() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q",
                "({!term _name=fantasy_cat f=cat_s}fantasy) OR ({!term _name=scifi_cat f=cat_s}scifi)",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==7",
        "/matched_queries_per_hit/1/[0]=='fantasy_cat'",
        "/matched_queries_per_hit/5/[0]=='scifi_cat'",
        "/matched_queries_summary/fantasy_cat/count==4",
        "/matched_queries_summary/scifi_cat/count==3");
  }

  /** An unnamed term query must produce no matched_queries output even when mq=true. */
  @Test
  public void testUnnamedQueryProducesNoOutput() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q", "{!term f=cat_s}fantasy",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==4",
        "!/matched_queries_per_hit==null",
        "!/matched_queries_summary==null");
  }

  /** Per-hit list carries exactly the names that match for that document. */
  @Test
  public void testMultiValuedFieldBothNamesPresent() throws Exception {
    // docs 2 and 3 match both fantasy_cat and childrens_cat
    assertJQ(
        req(
            "qt", HANDLER,
            "q",
                "({!term _name=fantasy_cat f=cat_s}fantasy) OR ({!term _name=childrens_cat f=cat_s}childrens)",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==4",
        "/matched_queries_summary/fantasy_cat/count==4",
        "/matched_queries_summary/childrens_cat/count==2",
        "/matched_queries_summary/childrens_cat/docIds/[0]=='2'",
        "/matched_queries_summary/childrens_cat/docIds/[1]=='3'");
  }

  /**
   * {@code {!terms}} with a single {@code _name}: all matching docs — across both index segments —
   * are tagged with that name.
   */
  @Test
  public void testTermsNamedQuery() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q", "{!terms _name=genre_all f=cat_s}fantasy,scifi",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==7",
        "/matched_queries_per_hit/1/[0]=='genre_all'",
        "/matched_queries_per_hit/5/[0]=='genre_all'",
        "/matched_queries_summary/genre_all/count==7",
        "/matched_queries_summary/genre_all/docIds/[0]=='1'",
        "/matched_queries_summary/genre_all/docIds/[6]=='7'");
  }

  /**
   * Outer {@code {!bool _name=...}} plus inner named {@code {!term _name=...}} SHOULD clauses: the
   * outer name appears on every hit; inner names appear only on the docs whose specific clause
   * fired. All three names are independent entries in the summary.
   */
  @Test
  public void testBoolOuterAndInnerNamesComposed() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q",
                "{!bool _name=all_books"
                    + "  should='{!term _name=fantasy_cat f=cat_s}fantasy'"
                    + "  should='{!term _name=scifi_cat  f=cat_s}scifi'}",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==7",
        // every doc carries all_books (outer name)
        "/matched_queries_summary/all_books/count==7",
        // inner names split correctly
        "/matched_queries_summary/fantasy_cat/count==4",
        "/matched_queries_summary/scifi_cat/count==3",
        // spot-check: doc 1 has both all_books and fantasy_cat
        "/matched_queries_per_hit/1/[0]=='all_books'",
        "/matched_queries_per_hit/1/[1]=='fantasy_cat'",
        // spot-check: doc 5 has both all_books and scifi_cat
        "/matched_queries_per_hit/5/[0]=='all_books'",
        "/matched_queries_per_hit/5/[1]=='scifi_cat'");
  }

  /**
   * {@code {!bool}} with multiple named SHOULD clauses: each doc is tagged only with the clause(s)
   * it actually matched — same semantics as an explicit OR but exercising the BoolQParserPlugin /
   * FiltersQParser code path.
   */
  @Test
  public void testBoolMultipleShouldNamedTerms() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q",
                "{!bool should='{!term _name=fantasy_cat f=cat_s}fantasy'"
                    + "     should='{!term _name=scifi_cat f=cat_s}scifi'}",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==7",
        "/matched_queries_per_hit/1/[0]=='fantasy_cat'",
        "/matched_queries_per_hit/4/[0]=='fantasy_cat'",
        "/matched_queries_per_hit/5/[0]=='scifi_cat'",
        "/matched_queries_per_hit/7/[0]=='scifi_cat'",
        "/matched_queries_summary/fantasy_cat/count==4",
        "/matched_queries_summary/scifi_cat/count==3");
  }

  /**
   * {@code {!bool}} with an unnamed MUST clause and a named SHOULD clause: the MUST clause drives
   * which docs are returned; the named SHOULD clause fires only for the subset that also matches
   * it. Docs that satisfy the MUST but not the SHOULD must be absent from {@code
   * matched_queries_per_hit} and must not inflate the summary count.
   */
  @Test
  public void testBoolMustWithNamedShould() throws Exception {
    // MUST: all 4 fantasy docs; named SHOULD: only docs 2 and 3 (childrens)
    assertJQ(
        req(
            "qt", HANDLER,
            "q",
                "{!bool must='{!term f=cat_s}fantasy'"
                    + "     should='{!term _name=childrens_cat f=cat_s}childrens'}",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==4",
        // docs 2 and 3 matched the named SHOULD
        "/matched_queries_per_hit/2/[0]=='childrens_cat'",
        "/matched_queries_per_hit/3/[0]=='childrens_cat'",
        // docs 1 and 4 matched only the unnamed MUST — no entry for them
        "!/matched_queries_per_hit/1==null",
        "!/matched_queries_per_hit/4==null",
        "/matched_queries_summary/childrens_cat/count==2",
        "/matched_queries_summary/childrens_cat/docIds/[0]=='2'",
        "/matched_queries_summary/childrens_cat/docIds/[1]=='3'");
  }

  /**
   * {@code {!prefix}} with {@code _name}: all fantasy docs (cat_s starting with "fanta") are
   * tagged.
   */
  @Test
  public void testPrefixNamedQuery() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q", "{!prefix _name=fanta_prefix f=cat_s}fanta",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==4",
        "/matched_queries_summary/fanta_prefix/count==4",
        "/matched_queries_per_hit/1/[0]=='fanta_prefix'",
        "/matched_queries_per_hit/4/[0]=='fanta_prefix'");
  }

  /**
   * {@code {!edismax}} with {@code _name}: extended DisMax query; all matching docs carry the name.
   */
  @Test
  public void testEdismaxNamedQuery() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q", "{!edismax _name=fantasy_edismax qf=cat_s}fantasy",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==4",
        "/matched_queries_summary/fantasy_edismax/count==4",
        "/matched_queries_per_hit/1/[0]=='fantasy_edismax'",
        "/matched_queries_per_hit/4/[0]=='fantasy_edismax'");
  }

  /**
   * {@code {!lucene}} with {@code _name}: standard Lucene query syntax; all matching docs tagged.
   */
  @Test
  public void testLuceneNamedQuery() throws Exception {
    assertJQ(
        req(
            "qt", HANDLER,
            "q", "{!lucene _name=scifi_lucene df=cat_s}scifi",
            "matched_queries", "true",
            "sort", "id asc",
            "rows", "10"),
        "/response/numFound==3",
        "/matched_queries_summary/scifi_lucene/count==3",
        "/matched_queries_per_hit/5/[0]=='scifi_lucene'",
        "/matched_queries_per_hit/7/[0]=='scifi_lucene'");
  }
}

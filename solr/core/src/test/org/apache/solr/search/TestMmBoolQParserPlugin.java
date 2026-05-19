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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NamedMatches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMmBoolQParserPlugin extends SolrTestCaseJ4 {

  private static BooleanQuery.Builder shouldBuilder(String... terms) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String term : terms) {
      builder.add(new TermQuery(new Term("name", term)), BooleanClause.Occur.SHOULD);
    }
    return builder;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  private static Query parseQuery(SolrQueryRequest req) throws Exception {
    QParser parser = QParser.getParser(req.getParams().get("q"), req);
    return parser.getQuery();
  }

  @Test
  public void testBooleanQuery() throws Exception {
    Query actual = parseQuery(req("q", "{!bool must=name:foo should=name:bar should=name:qux}"));

    BooleanQuery expected =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("name", "foo")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("name", "bar")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("name", "qux")), BooleanClause.Occur.SHOULD)
            .setMinimumNumberShouldMatch(0)
            .build();

    assertEquals(expected, actual);
  }

  @Test
  public void testMinShouldMatch() throws Exception {
    Query actual =
        parseQuery(req("q", "{!bool should=name:foo should=name:bar should=name:qux mm=2}"));

    BooleanQuery expected =
        shouldBuilder("foo", "bar", "qux").setMinimumNumberShouldMatch(2).build();

    assertEquals(expected, actual);
  }

  @Test
  public void testMinShouldMatchPercentage75() throws Exception {
    Query actual =
        parseQuery(req("q", "{!bool should=name:foo should=name:bar should=name:qux mm=75%}"));

    BooleanQuery expected =
        shouldBuilder("foo", "bar", "qux").setMinimumNumberShouldMatch(2).build();

    assertEquals(expected, actual);
  }

  @Test
  public void testMinShouldMatchPercentage50() throws Exception {
    Query actual =
        parseQuery(req("q", "{!bool should=name:foo should=name:bar should=name:qux mm=50%}"));

    BooleanQuery expected =
        shouldBuilder("foo", "bar", "qux").setMinimumNumberShouldMatch(1).build();

    assertEquals(expected, actual);
  }

  @Test
  public void testMinShouldMatchThresholdsLower() throws Exception {
    Query actual =
        parseQuery(
            req("q", "{!bool should=name:foo should=name:bar should=name:qux mm='2<-1 5<-2'}"));

    BooleanQuery expected =
        shouldBuilder("foo", "bar", "qux").setMinimumNumberShouldMatch(2).build();

    assertEquals(expected, actual);
  }

  @Test
  public void testMinShouldMatchThresholdsUpper() throws Exception {
    Query actual =
        parseQuery(
            req(
                "q",
                "{!bool should=name:foo should=name:bar should=name:qux should=name:n1 should=name:n2 should=name:n3 mm='2<-1 5<-2'}"));

    BooleanQuery expected =
        shouldBuilder("foo", "bar", "qux", "n1", "n2", "n3").setMinimumNumberShouldMatch(4).build();

    assertEquals(expected, actual);
  }

  @Test
  public void testNoClauses() throws Exception {
    Query actual = parseQuery(req("q", "{!bool}"));

    BooleanQuery expected = new BooleanQuery.Builder().build();
    assertEquals(expected, actual);
  }

  @Test
  public void testExcludeTags() throws Exception {
    Query actual =
        parseQuery(
            req(
                "q",
                "{!bool must=$ref excludeTags=t2}",
                "ref",
                "{!tag=t1}foo",
                "ref",
                "{!tag=t2}bar",
                "df",
                "name"));

    BooleanQuery expected =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("name", "foo")), BooleanClause.Occur.MUST)
            .build();
    assertEquals(expected, actual);
  }

  @Test
  public void testNamedBoolQuery() throws Exception {
    Query actual = parseQuery(req("q", "{!bool _name=my_bool must=name:foo should=name:bar}"));

    BooleanQuery inner =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("name", "foo")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("name", "bar")), BooleanClause.Occur.SHOULD)
            .setMinimumNumberShouldMatch(0)
            .build();
    assertEquals(NamedMatches.wrapQuery("my_bool", inner), actual);
  }

  @Test
  public void testNamedBoolQueryWithMinShouldMatch() throws Exception {
    Query actual =
        parseQuery(
            req(
                "q",
                "{!bool _name=at_least_two should=name:foo should=name:bar should=name:qux mm=2}"));

    BooleanQuery inner =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("name", "foo")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("name", "bar")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("name", "qux")), BooleanClause.Occur.SHOULD)
            .setMinimumNumberShouldMatch(2)
            .build();
    assertEquals(NamedMatches.wrapQuery("at_least_two", inner), actual);
  }

  @Test
  public void testNamedBoolQueryWithExcludeTags() throws Exception {
    // excludeTags filters one of the $ref clauses; _name wraps what remains
    Query actual =
        parseQuery(
            req(
                "q", "{!bool _name=my_ref must=$ref excludeTags=t2}",
                "ref", "{!tag=t1}foo",
                "ref", "{!tag=t2}bar",
                "df", "name"));
    BooleanQuery inner =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("name", "foo")), BooleanClause.Occur.MUST)
            .build();
    assertEquals(NamedMatches.wrapQuery("my_ref", inner), actual);
  }

  @Test
  public void testInvalidMinShouldMatchThrowsException() {
    expectThrows(
        SolrException.class,
        NumberFormatException.class,
        () -> parseQuery(req("q", "{!bool should=name:foo mm=2.9}")));
  }
}

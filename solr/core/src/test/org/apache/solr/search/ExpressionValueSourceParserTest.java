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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.ExpressionValueSourceParser.SolrBindings;
import org.apache.solr.util.DateMathParser;
import org.junit.BeforeClass;

public class ExpressionValueSourceParserTest extends SolrTestCaseJ4 {

  private final List<DoubleValuesSource> positionalArgs = new ArrayList<>();

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-expressions-vs.xml", "schema15.xml");

    assertU(
        adoc("id", "1", "int1_i", "50", "double1_d", "-2.5", "date1_dt", "1996-12-19T16:39:57Z"));
    assertU(
        adoc("id", "2", "int1_i", "-30", "double1_d", "10.3", "date1_dt", "1999-12-19T16:39:57Z"));
    assertU(
        adoc("id", "3", "int1_i", "10", "double1_d", "500.3", "date1_dt", "1995-12-19T16:39:57Z"));
    assertU(adoc("id", "4", "int1_i", "40", "double1_d", "-1", "date1_dt", "1994-12-19T16:39:57Z"));
    assertU(
        adoc("id", "5", "int1_i", "20", "double1_d", "2.1", "date1_dt", "1997-12-19T16:39:57Z"));

    assertU(commit());
  }

  public void testValidBindings() throws ParseException {
    IndexSchema schema = h.getCore().getLatestSchema();
    SolrBindings bindings = new SolrBindings("ScOrE", schema, this.positionalArgs);

    assertEquals(
        "foo_i from bindings is wrong",
        schema
            .getFieldType("foo_i")
            .getValueSource(schema.getField("foo_i"), null)
            .asDoubleValuesSource(),
        bindings.getDoubleValuesSource("foo_i"));
    ValueSource scoreBind =
        ValueSource.fromDoubleValuesSource(bindings.getDoubleValuesSource("ScOrE"));
    assertNotNull("ScOrE bindings failed", scoreBind);

    try {
      bindings.getDoubleValuesSource("not_a_expr_and_not_in_schema");
      fail("no exception from bogus binding");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "wrong exception message: " + e.getMessage(),
          e.getMessage().contains("not_a_expr_and_not_in_schema"));
    }

    // change things up a bit
    bindings = new SolrBindings(null, schema, this.positionalArgs);
    try {
      bindings.getDoubleValuesSource("ScOrE");
      fail("ScOrE should not have bindings");
    } catch (IllegalArgumentException e) {
      // NOOP
    }
  }

  public void testBogusBindings() throws ParseException {
    IndexSchema schema = h.getCore().getLatestSchema();
    SolrBindings bindings = new SolrBindings("ScOrE", schema, this.positionalArgs);

    try {
      bindings.getDoubleValuesSource("yak");
      fail("sanity check failed: yak has a binding?");
    } catch (IllegalArgumentException e) {
      // NOOP
    }

    // change things up a bit
    bindings = new SolrBindings(null, schema, this.positionalArgs);
    try {
      bindings.getDoubleValuesSource("ScOrE");
      fail("ScOrE should not have bindings");
    } catch (IllegalArgumentException e) {
      // NOOP
    }
    try {
      bindings.getDoubleValuesSource("score");
      fail("score should not have bindings");
    } catch (IllegalArgumentException e) {
      // NOOP
    }
  }

  /** tests an expression referring to a score field using an overridden score binding */
  public void testSortSsccoorree() {
    assertQ(
        "sort",
        req(
            "fl", "id",
            "q", "{!func}field(int1_i)",
            "sort", "expr_ssccoorree() desc,id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[5]/str[@name='id'][.='2']");
  }

  /** tests a constant expression */
  public void testSortConstant() {
    assertQ(
        "sort",
        req("fl", "id", "q", "*:*", "sort", "sin1() desc,id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='4']",
        "//result/doc[5]/str[@name='id'][.='5']");
  }

  // Removed testSortExpression as expressions can no longer reference other expressions

  /** tests an expression referring to an int field */
  public void testSortInt() {
    assertQ(
        "sort",
        req("fl", "id", "q", "*:*", "sort", "sqrt_int1_i() desc,id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='2']", // NaN
        "//result/doc[2]/str[@name='id'][.='1']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[5]/str[@name='id'][.='3']");
  }

  /** tests an expression referring to a double field */
  public void testSortDouble() {
    assertQ(
        "sort",
        req("fl", "id", "q", "*:*", "sort", "sqrt_double1_d() desc,id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='1']", // NaN
        "//result/doc[2]/str[@name='id'][.='4']", // NaN
        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='2']",
        "//result/doc[5]/str[@name='id'][.='5']");
  }

  /** tests an expression referring to a date field */
  public void testSortDate() {
    assertQ(
        "sort",
        req("fl", "id", "q", "*:*", "sort", "date1_dt_minus_1990() desc,id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[3]/str[@name='id'][.='1']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[5]/str[@name='id'][.='4']");
  }

  /** tests an expression referring to a score field */
  public void testSortScore() {
    assertQ(
        "sort",
        req("fl", "id", "q", "{!func}field(int1_i)", "sort", "one_plus_score() desc,id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[5]/str[@name='id'][.='2']");
  }

  /** tests a constant expression */
  public void testReturnConstant() {
    final float expected = (float) Math.sin(1);
    assertQ(
        "return",
        req("fl", "sin1:sin1()", "q", "*:*", "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='sin1'][.='" + expected + "']",
        "//result/doc[2]/float[@name='sin1'][.='" + expected + "']",
        "//result/doc[3]/float[@name='sin1'][.='" + expected + "']",
        "//result/doc[4]/float[@name='sin1'][.='" + expected + "']",
        "//result/doc[5]/float[@name='sin1'][.='" + expected + "']");
  }

  // Removed testReturnExpression as expressions can no longer reference other expressions

  /** tests an expression referring to an int field */
  public void testReturnInt() {
    assertQ(
        "return",
        req("fl", "foo:sqrt_int1_i()", "q", "*:*", "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='foo'][.=" + (float) Math.sqrt(50) + "]",
        "//result/doc[2]/float[@name='foo'][.='NaN']",
        "//result/doc[3]/float[@name='foo'][.=" + (float) Math.sqrt(10) + "]",
        "//result/doc[4]/float[@name='foo'][.=" + (float) Math.sqrt(40) + "]",
        "//result/doc[5]/float[@name='foo'][.=" + (float) Math.sqrt(20) + "]");
  }

  /** tests an expression referring to a double field */
  public void testReturnDouble() {
    assertQ(
        "return",
        req("fl", "bar:sqrt_double1_d()", "q", "*:*", "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='bar'][.='NaN']",
        "//result/doc[2]/float[@name='bar'][.=" + (float) Math.sqrt(10.3d) + "]",
        "//result/doc[3]/float[@name='bar'][.=" + (float) Math.sqrt(500.3d) + "]",
        "//result/doc[4]/float[@name='bar'][.='NaN']",
        "//result/doc[5]/float[@name='bar'][.=" + (float) Math.sqrt(2.1d) + "]");
  }

  /** tests an expression referring to a date field */
  public void testReturnDate() {
    assertQ(
        "return",
        req("fl", "date1_dt_minus_1990:date1_dt_minus_1990()", "q", "*:*", "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='date1_dt_minus_1990'][.='"
            + (float)
                (DateMathParser.parseMath(null, "1996-12-19T16:39:57Z").getTime() - 631036800000D)
            + "']",
        "//result/doc[2]/float[@name='date1_dt_minus_1990'][.='"
            + (float)
                (DateMathParser.parseMath(null, "1999-12-19T16:39:57Z").getTime() - 631036800000D)
            + "']",
        "//result/doc[3]/float[@name='date1_dt_minus_1990'][.='"
            + (float)
                (DateMathParser.parseMath(null, "1995-12-19T16:39:57Z").getTime() - 631036800000D)
            + "']",
        "//result/doc[4]/float[@name='date1_dt_minus_1990'][.='"
            + (float)
                (DateMathParser.parseMath(null, "1994-12-19T16:39:57Z").getTime() - 631036800000D)
            + "']",
        "//result/doc[5]/float[@name='date1_dt_minus_1990'][.='"
            + (float)
                (DateMathParser.parseMath(null, "1997-12-19T16:39:57Z").getTime() - 631036800000D)
            + "']");
  }

  /** tests an expression referring to score */
  public void testReturnScores() {
    assertQ(
        "return",
        // unfortunately, need to add fl=score for ValueSourceAugmenter to access it
        req(
            "fl", "one_plus_score(),score",
            "q", "{!func}field(int1_i)",
            "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='one_plus_score()'][.='51.0']",
        "//result/doc[2]/float[@name='one_plus_score()'][.='1.0']",
        "//result/doc[3]/float[@name='one_plus_score()'][.='11.0']",
        "//result/doc[4]/float[@name='one_plus_score()'][.='41.0']",
        "//result/doc[5]/float[@name='one_plus_score()'][.='21.0']");
  }

  public void testReturnScores2() {
    assertQ(
        "return",
        // unfortunately, need to add fl=score for ValueSourceAugmenter to access it
        req(
            "fl", "two_plus_score:two_plus_score(),score",
            "q", "{!func}field(int1_i)",
            "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='two_plus_score'][.='52.0']",
        "//result/doc[2]/float[@name='two_plus_score'][.='2.0']",
        "//result/doc[3]/float[@name='two_plus_score'][.='12.0']",
        "//result/doc[4]/float[@name='two_plus_score'][.='42.0']",
        "//result/doc[5]/float[@name='two_plus_score'][.='22.0']");
  }

  public void testReturnScores3() {
    assertQ(
        "return",
        // unfortunately, need to add fl=score for ValueSourceAugmenter to access it
        req(
            "fl", "foo:sqrt_int1_i_plus_one_plus_score(),score",
            "q", "{!func}field(int1_i)",
            "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='foo'][.='" + (float) (Math.sqrt(50) + 1 + 50) + "']",
        "//result/doc[2]/float[@name='foo'][.='NaN']",
        "//result/doc[3]/float[@name='foo'][.='" + (float) (Math.sqrt(10) + 1 + 10) + "']",
        "//result/doc[4]/float[@name='foo'][.='" + (float) (Math.sqrt(40) + 1 + 40) + "']",
        "//result/doc[5]/float[@name='foo'][.='" + (float) (Math.sqrt(20) + 1 + 20) + "']");
  }

  /** tests an expression with positional arguments */
  public void testPositionalArgs() {
    assertQ(
        "return",
        req(
            "fl", "sum:positional_args(10,20,30)",
            "q", "*:*",
            "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='sum'][.='60.0']",
        "//result/doc[2]/float[@name='sum'][.='60.0']",
        "//result/doc[3]/float[@name='sum'][.='60.0']",
        "//result/doc[4]/float[@name='sum'][.='60.0']",
        "//result/doc[5]/float[@name='sum'][.='60.0']");
  }

  /** tests embedding an expression in another value source */
  public void testEmbeddedExpression() {
    final float expected = (float) Math.sin(1);
    assertQ(
        "return",
        req(
            "fl", "embedded:sum(sin1(),5)",
            "q", "*:*",
            "sort", "id asc"),
        "//*[@numFound='5']",
        "//result/doc[1]/float[@name='embedded'][.='" + (expected + 5.0f) + "']",
        "//result/doc[2]/float[@name='embedded'][.='" + (expected + 5.0f) + "']",
        "//result/doc[3]/float[@name='embedded'][.='" + (expected + 5.0f) + "']",
        "//result/doc[4]/float[@name='embedded'][.='" + (expected + 5.0f) + "']",
        "//result/doc[5]/float[@name='embedded'][.='" + (expected + 5.0f) + "']");
  }
}

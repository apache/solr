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
package org.apache.solr.search.function;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

public class ScoreFunctionTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-functionquery.xml", "schema11.xml");

    addDocsInRandomOrder();
  }

  private static void addDocsInRandomOrder() {
    // add documents to the collection, but randomize
    // the number of segments, and which specific docs
    // are added to each segment - this is especially
    // useful for testing score function in pseudo fields
    List<String> updates =
        Arrays.asList(
            commit(),
            commit(),
            commit(),
            adoc("id", "1", "text", "foo"),
            adoc("id", "2", "text", "bar"),
            adoc("id", "3", "text", "qux"),
            adoc("id", "4", "text", "asd"),
            adoc("id", "101", "text", "random text"),
            adoc("id", "102", "text", "random text"),
            adoc("id", "103", "text", "random text"),
            adoc("id", "104", "text", "random text"),
            adoc("id", "105", "text", "random text"),
            adoc("id", "106", "text", "random text"),
            adoc("id", "107", "text", "random text"),
            adoc("id", "108", "text", "random text"),
            adoc("id", "109", "text", "random text"));

    Collections.shuffle(updates, random());

    for (var update : updates) {
      assertU(update);
    }

    assertU(commit());
  }

  @Test
  public void testScoreFunction_boostQuery() throws Exception {
    assertJQ(
        req("q", "foo^=1 bar^=2 qux^=3 asd^=4", "df", "text", "fl", "id,score"),
        "/response/numFound==4",
        "/response/docs/[0]/id=='4'",
        "/response/docs/[0]/score==4.0",
        "/response/docs/[1]/id=='3'",
        "/response/docs/[1]/score==3.0",
        "/response/docs/[2]/id=='2'",
        "/response/docs/[2]/score==2.0",
        "/response/docs/[3]/id=='1'",
        "/response/docs/[3]/score==1.0");

    // boost function that relies on score()
    assertJQ(
        req(
            "q",
            "{!boost b=if(lte(score(),2),2.5,1)}foo^=1 bar^=2 qux^=3 asd^=4",
            "df",
            "text",
            "fl",
            "id,score"),
        "/response/numFound==4",
        "/response/docs/[0]/id=='2'",
        "/response/docs/[0]/score==5.0",
        "/response/docs/[1]/id=='4'",
        "/response/docs/[1]/score==4.0",
        "/response/docs/[2]/id=='3'",
        "/response/docs/[2]/score==3.0",
        "/response/docs/[3]/id=='1'",
        "/response/docs/[3]/score==2.5");
  }

  @Test
  public void testScoreFunction_postFilter() throws Exception {
    // frange query as postfilter
    assertJQ(
        req(
            "q",
            "foo^=1 bar^=2 qux^=3 asd^=4",
            "df",
            "text",
            "fl",
            "id,score",
            "fq",
            "{!frange l=2 u=3 cache=false}score()"),
        "/response/numFound==2",
        "/response/docs/[0]/id=='3'",
        "/response/docs/[0]/score==3.0",
        "/response/docs/[1]/id=='2'",
        "/response/docs/[1]/score==2.0");

    // doesn't work if not a postfilter
    assertThrows(
        SolrException.class,
        () ->
            assertJQ(
                req(
                    "q",
                    "foo^=1 bar^=2 qux^=3 asd^=4",
                    "df",
                    "text",
                    "fl",
                    "id,score",
                    "fq",
                    "{!frange l=2 u=3}score()")));
  }

  @Test
  public void testScoreFunction_pseudoField() throws Exception {
    assertJQ(
        req(
            "q", "foo^=1 bar^=2 qux^=3",
            "df", "text",
            "fl", "id,score,custom:add(1,score(),score())"),
        "/response/numFound==3",
        "/response/docs/[0]/id=='3'",
        "/response/docs/[0]/score==3.0",
        "/response/docs/[0]/custom==7.0",
        "/response/docs/[1]/id=='2'",
        "/response/docs/[1]/score==2.0",
        "/response/docs/[1]/custom==5.0",
        "/response/docs/[2]/id=='1'",
        "/response/docs/[2]/score==1.0",
        "/response/docs/[2]/custom==3.0");

    // error if scores not enabled
    assertThrows(
        SolrException.class,
        () ->
            assertJQ(
                req(
                    "q", "foo^=1 bar^=2 qux^=3",
                    "df", "text",
                    "fl", "id,custom:add(1,score(),score())")));
  }

  @Test
  public void testScoreFunction_nested() throws Exception {
    assertJQ(
        req(
            "json",
                """
                {
                  "query": {
                    "boost": {
                      "b": "if(lte(score(),6),1,0.1)",
                      "query": {
                        "boost": {
                          "b": "if(gte(score(),3),2,1)",
                          "query": "foo^=1 bar^=2 qux^=3 asd^=4"
                        }
                      }
                    }
                  }
                }""",
            "df", "text",
            "fl", "id,score"),
        "/response/numFound==4",
        "/response/docs/[0]/id=='3'",
        "/response/docs/[0]/score==6.0",
        "/response/docs/[1]/id=='2'",
        "/response/docs/[1]/score==2.0",
        "/response/docs/[2]/id=='1'",
        "/response/docs/[2]/score==1.0",
        "/response/docs/[3]/id=='4'",
        "/response/docs/[3]/score==0.8");

    assertJQ(
        req(
            "json",
                """
                {
                  "query": {
                    "boost": {
                      "b": "if(gte(score(),10),0.1,1)",
                      "query": {
                        "boost": {
                          "b": "if(eq(query($q1),score()),100,1)",
                          "query": "foo^=1 bar^=2 qux^=3 asd^=4"
                        }
                      }
                    }
                  },
                  "queries": {
                    "q1": {
                      "boost": {
                        "b": "if(eq(score(),3),1,2)",
                        "query": "qux^=3 asd^=4"
                      }
                    }
                  }
                }""",
            "df", "text",
            "fl", "id,score"),
        "/response/numFound==4",
        "/response/docs/[0]/id=='3'",
        "/response/docs/[0]/score==30.0",
        "/response/docs/[1]/id=='4'",
        "/response/docs/[1]/score==4.0",
        "/response/docs/[2]/id=='2'",
        "/response/docs/[2]/score==2.0",
        "/response/docs/[3]/id=='1'",
        "/response/docs/[3]/score==1.0");
  }

  @Test
  public void testScoreFunction_combined() throws Exception {
    assertJQ(
        req(
            "q", "{!boost b=if(gte(score(),3),2,1) v=$qq}",
            "qq", "foo^=1 bar^=2 qux^=3 asd^=4",
            "fq", "{!frange cache=false u=7}score()",
            "df", "text",
            "fl", "id,score,score_plus_one:add(1,score())"),
        "/response/numFound==3",
        "/response/docs/[0]/id=='3'",
        "/response/docs/[0]/score==6.0",
        "/response/docs/[0]/score_plus_one==7.0",
        "/response/docs/[1]/id=='2'",
        "/response/docs/[1]/score==2.0",
        "/response/docs/[1]/score_plus_one==3.0",
        "/response/docs/[2]/id=='1'",
        "/response/docs/[2]/score==1.0",
        "/response/docs/[2]/score_plus_one==2.0");
  }
}

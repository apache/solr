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

/**
 * Unit tests for prefix-query functionality - mostly testing the 'minPrefixLength' setting
 * available in solrconfig.xml
 */
public class PrefixQueryTest extends SolrTestCaseJ4 {

  private static final String[] FIELDS_TO_TEST_PREFIX_LIMITING = new String[] {"val_s", "t_val"};

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("solr.query.minPrefixLength", "2");
    initCore("solrconfig.xml", "schema.xml");

    assertU(createDocWithFieldVal("1", "aaa"));
    assertU(createDocWithFieldVal("2", "aab"));
    assertU(createDocWithFieldVal("3", "aac"));
    assertU(createDocWithFieldVal("4", "abc"));

    assertU(createDocWithFieldVal("5", "bbb"));
    assertU(createDocWithFieldVal("6", "bbc"));

    assertU("<commit/>");
  }

  // Sanity-check of a few queries we'll use in other tests
  @Test
  public void testPrefixQueryMatchesExpectedDocuments() {
    for (String fieldName : FIELDS_TO_TEST_PREFIX_LIMITING) {
      assertQ(req(fieldName + ":*"), "//*[@numFound='6']");
      assertQ(req(fieldName + ":aa*"), "//*[@numFound='3']");
      assertQ(req(fieldName + ":bb*"), "//*[@numFound='2']");
    }
  }

  @Test
  public void testPrefixQueryObeysMinPrefixLimit() {
    for (String fieldName : FIELDS_TO_TEST_PREFIX_LIMITING) {
      assertQEx(
          "Prefix query didn't obey limit",
          "does not meet the minimum prefix length [2] (actual=[1])",
          req(fieldName + ":a*"),
          SolrException.ErrorCode.BAD_REQUEST);
    }
  }

  @Test
  public void testPrefixQParserObeysMinPrefixLimit() {
    for (String fieldName : FIELDS_TO_TEST_PREFIX_LIMITING) {
      assertQEx(
          "Prefix query didn't obey limit",
          "does not meet the minimum prefix length [2] (actual=[1])",
          req("q", "{!prefix f=" + fieldName + "}a"),
          SolrException.ErrorCode.BAD_REQUEST);
    }
  }

  @Test
  public void testComplexPhraseQParserObeysMinPrefixLimit() {
    for (String fieldName : FIELDS_TO_TEST_PREFIX_LIMITING) {
      assertQEx(
          "{!complex} query didn't obey min-prefix limit",
          "does not meet the minimum prefix length [2] (actual=[1])",
          req("q", "{!complexphrase inOrder=true}" + fieldName + ":\"a*\""),
          SolrException.ErrorCode.BAD_REQUEST);
    }
  }

  @Test
  public void testLocalParamCanBeUsedToOverrideConfiguredLimit() {
    // The solrconfig.xml configured limit is '2'; requests should fail when that is not overridden
    for (String fieldName : FIELDS_TO_TEST_PREFIX_LIMITING) {
      assertQEx(
          "{!complex} query didn't obey min-prefix limit",
          "does not meet the minimum prefix length [2] (actual=[1])",
          req("q", "{!complexphrase inOrder=true}" + fieldName + ":\"a*\""),
          SolrException.ErrorCode.BAD_REQUEST);
    }

    // When the configured limit *is* overridden to be more lenient, the requests should succeed!
    for (String fieldName : FIELDS_TO_TEST_PREFIX_LIMITING) {
      assertQ(
          req(
              "q",
              "{!complexphrase inOrder=true minPrefixQueryTermLength=-1}" + fieldName + ":\"a*\""),
          "//*[@numFound='4']");
    }
  }

  @Test
  public void testQuestionMarkWildcardsCountTowardsMinimumPrefix() {
    // Both of these queries succeed since the '?' wildcard is counted as a part of the prefix
    assertQ(req("val_s:a?c*"), "//*[@numFound='2']"); // Matches 'aac' and 'abc'
    assertQ(req("val_s:a??*"), "//*[@numFound='4']"); // Matches all documents starting with 'a'
  }

  private static String createDocWithFieldVal(String id, String fieldVal) {
    return "<add><doc><field name=\"id\">"
        + id
        + "</field><field name=\"val_s\">"
        + fieldVal
        + "</field><field name=\"t_val\">"
        + fieldVal
        + "</field></doc></add>";
  }
}

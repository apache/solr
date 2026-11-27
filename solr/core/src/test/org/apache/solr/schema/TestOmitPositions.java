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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;

public class TestOmitPositions extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    // add some docs
    assertU(
        adoc(
            "id",
            "1",
            "nopositionstext",
            "this is a test this is only a test",
            "text",
            "just another test"));
    assertU(
        adoc(
            "id",
            "2",
            "nopositionstext",
            "test test test test test test test test test test test test test",
            "text",
            "have a nice day"));
    assertU(commit());
  }

  public void testFrequencies() {
    // doc 2 should be ranked above doc 1
    assertQ(
        "term query: ",
        req("fl", "id", "q", "nopositionstext:test"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]");
  }

  /**
   * An explicit phrase against a field indexed without positions is a client/schema mismatch, not a
   * server fault: it must be rejected with 400.
   */
  public void testExplicitPhraseIsBadRequest() {
    assertQEx(
        "lucene parser: explicit phrase on omitPositions field",
        req("fl", "id", "q", "nopositionstext:\"test test\""),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "edismax: explicit phrase via qf on omitPositions field",
        req("fl", "id", "defType", "edismax", "qf", "nopositionstext", "q", "\"test test\""),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  /**
   * pf/pf2/pf3 are implicit, best-effort boosts the user never typed. A phrase field that cannot
   * support phrases must not fail the whole request; the boost should simply not apply to it.
   */
  public void testImplicitPhraseBoostIsSkipped() {
    for (String pf : new String[] {"pf", "pf2", "pf3"}) {
      assertQ(
          pf + " on omitPositions field must not fail the request",
          req(
              "fl",
              "id",
              "defType",
              "edismax",
              "qf",
              "nopositionstext",
              pf,
              "nopositionstext",
              "q",
              "this is a test"),
          "//*[@numFound='2']");
    }
    // a valid phrase field alongside the bad one should still be applied
    assertQ(
        "pf mixing positional and non-positional fields",
        req(
            "fl", "id",
            "defType", "edismax",
            "qf", "text nopositionstext",
            "pf", "text nopositionstext",
            "q", "just another"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=1]");
  }
}

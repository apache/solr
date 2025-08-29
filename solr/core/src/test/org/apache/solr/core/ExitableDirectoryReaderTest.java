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
package org.apache.solr.core;

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.CallerSpecificQueryLimit;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Test;

public class ExitableDirectoryReaderTest extends SolrTestCaseJ4 {
  static final int NUM_DOCS = 100;
  static final String assertionString = "/response/numFound==" + NUM_DOCS;
  static final String failureAssertionString = "/responseHeader/partialResults==true]";

  public static void createIndex() {
    for (int i = 0; i < NUM_DOCS; i++) {
      assertU(
          adoc(
              "id",
              Integer.toString(i),
              "name",
              "a" + i + " b" + i + " c" + i + " d" + i + " e" + i));
      if (random().nextInt(NUM_DOCS) == 0) {
        assertU(commit()); // sometimes make multiple segments
      }
    }
    assertU(commit());
  }

  @After
  public void tearDownCore() {
    deleteCore();
  }

  @Test
  public void testWithExitableDirectoryReader() throws Exception {
    doTestWithExitableDirectoryReader(true);
  }

  @Test
  public void testWithoutExitableDirectoryReader() throws Exception {
    doTestWithExitableDirectoryReader(false);
  }

  private void doTestWithExitableDirectoryReader(boolean withExitableDirectoryReader)
      throws Exception {
    System.setProperty(
        SolrIndexSearcher.EXITABLE_READER_PROPERTY, String.valueOf(withExitableDirectoryReader));
    initCore("solrconfig-delaying-component.xml", "schema_latest.xml");
    createIndex();

    // create a limit that will not trip but will report calls to shouldExit()
    // NOTE: we need to use the inner class name to capture the calls
    String callerExpr = "ExitableTermsEnum:-1";
    CallerSpecificQueryLimit queryLimit = new CallerSpecificQueryLimit(callerExpr);
    TestInjection.queryTimeout = queryLimit;
    String q = "name:a*";
    assertJQ(req("q", q), assertionString);
    Map<String, Integer> callCounts = queryLimit.getCallCounts();
    if (withExitableDirectoryReader) {
      assertTrue(
          "there should be some calls from ExitableTermsEnum: " + callCounts,
          callCounts.get(callerExpr) > 0);
    } else {
      assertEquals(
          "there should be no calls from ExitableTermsEnum", 0, (int) callCounts.get(callerExpr));
    }

    // check that the limits are tripped in ExitableDirectoryReader if it's in use
    int maxCount = random().nextInt(10) + 1;
    callerExpr = "ExitableTermsEnum:" + maxCount;
    queryLimit = new CallerSpecificQueryLimit(callerExpr);
    TestInjection.queryTimeout = queryLimit;
    // avoid using the cache
    q = "name:b*";
    if (withExitableDirectoryReader) {
      assertJQ(req("q", q), failureAssertionString);
    } else {
      assertJQ(req("q", q), assertionString);
    }
    callCounts = queryLimit.getCallCounts();
    if (withExitableDirectoryReader) {
      assertTrue(
          "there should be at least " + maxCount + " calls from ExitableTermsEnum: " + callCounts,
          callCounts.get(callerExpr) >= maxCount);
    } else {
      assertEquals(
          "there should be no calls from ExitableTermsEnum", 0, (int) callCounts.get(callerExpr));
    }
  }
}

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
package org.apache.solr.analytics.facet;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.analytics.SolrAnalyticsTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryFacetTest extends SolrAnalyticsFacetTestCase {

  @BeforeClass
  public static void populate() {
    SolrAnalyticsTestCase.populateDocsForAnalyticsTests();
  }

  @Test
  public void queryFacetTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("mean", "mean(int_i)");
    expressions.put("count", "count(string_sm)");

    // Value Facet "with_missing"
    addFacet(
        "with_missing",
        "{ 'type':'query', 'queries':{'q1':'long_l:1 AND float_f:1.0','q2':'double_dm:[3 TO 11] OR double_dm:1'}}");

    addFacetValue("q1");
    addFacetResult("mean", 4.0);
    addFacetResult("count", 8L);

    addFacetValue("q2");
    addFacetResult("mean", 38.0 / 11.0);
    addFacetResult("count", 18L);

    testGrouping(expressions);
  }
}

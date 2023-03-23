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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.solr.analytics.SolrAnalyticsTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

@ThreadLeakLingering(linger = 0)
public class PivotFacetTest extends SolrAnalyticsFacetTestCase {

  @BeforeClass
  public static void populate() {
    SolrAnalyticsTestCase.populateDocsForAnalyticsTests();
  }

  @Test
  public void pivotFacetTest() throws Exception {
    String analyticsRequest =
        "{"
            + "\n 'groupings': { "
            + "\n   'grouping' : { "
            + "\n     'expressions' : { "
            + "\n       'mean' : 'mean(fill_missing(int_i,0))', "
            + "\n       'count' : 'count(long_lm)' "
            + "\n     }, "
            + "\n     'facets' : { "
            + "\n       'pivoting' : { "
            + "\n         'type' : 'pivot', "
            + "\n         'pivots' : [ "
            + "\n           { "
            + "\n             'name' : 'strings', "
            + "\n             'expression' : 'string_sm', "
            + "\n             'sort' : { "
            + "\n               'criteria' : [ "
            + "\n                 { "
            + "\n                   'type' : 'expression', "
            + "\n                   'expression' : 'mean', "
            + "\n                   'direction' : 'ascending' "
            + "\n                 }, "
            + "\n                 { "
            + "\n                   'type' : 'facetvalue', "
            + "\n                   'direction' : 'descending' "
            + "\n                 } "
            + "\n               ], "
            + "\n               'limit' : 3, "
            + "\n               'offset' : 1 "
            + "\n             } "
            + "\n           }, "
            + "\n           { "
            + "\n             'name' : 'date', "
            + "\n             'expression' : 'fill_missing(date_dt, \\'No Date\\')', "
            + "\n             'sort' : { "
            + "\n               'criteria' : [ "
            + "\n                 { "
            + "\n                   'type' : 'expression', "
            + "\n                   'expression' : 'count', "
            + "\n                   'direction' : 'ascending' "
            + "\n                 }, "
            + "\n                 { "
            + "\n                   'type' : 'expression', "
            + "\n                   'expression' : 'mean', "
            + "\n                   'direction' : 'ascending' "
            + "\n                 } "
            + "\n               ], "
            + "\n               'limit' : 2 "
            + "\n             } "
            + "\n           } "
            + "\n         ] "
            + "\n       } "
            + "\n     } "
            + "\n   } "
            + "\n } "
            + "\n} ";

    String test =
        "groupings=={'grouping':{'pivoting':["
            + "\n { "
            + "\n   'pivot' : 'strings', "
            + "\n   'value' : 'str3', "
            + "\n   'results' : { "
            + "\n     'mean' : 2.6, "
            + "\n     'count' : 10 "
            + "\n   }, "
            + "\n   'children' : [ "
            + "\n     { "
            + "\n       'pivot' : 'date', "
            + "\n       'value' : '1802-12-31T23:59:59Z', "
            + "\n       'results' : { "
            + "\n         'mean' : 4.0, "
            + "\n         'count' : 2 "
            + "\n       } "
            + "\n     }, "
            + "\n     { "
            + "\n       'pivot' : 'date', "
            + "\n       'value' : 'No Date', "
            + "\n       'results' : { "
            + "\n         'mean' : 2.0, "
            + "\n         'count' : 4 "
            + "\n       } "
            + "\n     } "
            + "\n   ]"
            + "\n }, "
            + "\n { "
            + "\n   'pivot' : 'strings', "
            + "\n   'value' : 'str2_second', "
            + "\n   'results' : { "
            + "\n     'mean' : 3.0, "
            + "\n     'count' : 0 "
            + "\n   }, "
            + "\n   'children' : [ "
            + "\n     { "
            + "\n       'pivot' : 'date', "
            + "\n       'value' : '1802-12-31T23:59:59Z', "
            + "\n       'results' : { "
            + "\n         'mean' : 1.0, "
            + "\n         'count' : 0 "
            + "\n       } "
            + "\n     }, "
            + "\n     { "
            + "\n       'pivot' : 'date', "
            + "\n       'value' : '1801-12-31T23:59:59Z', "
            + "\n       'results' : { "
            + "\n         'mean' : 3.0, "
            + "\n         'count' : 0 "
            + "\n       } "
            + "\n     } "
            + "\n   ]"
            + "\n }, "
            + "\n { "
            + "\n   'pivot' : 'strings', "
            + "\n   'value' : 'str2', "
            + "\n   'results' : { "
            + "\n     'mean' : 3.0, "
            + "\n     'count' : 0 "
            + "\n   }, "
            + "\n   'children' : [ "
            + "\n     { "
            + "\n       'pivot' : 'date', "
            + "\n       'value' : '1802-12-31T23:59:59Z', "
            + "\n       'results' : { "
            + "\n         'mean' : 1.0, "
            + "\n         'count' : 0 "
            + "\n       } "
            + "\n     }, "
            + "\n     { "
            + "\n       'pivot' : 'date', "
            + "\n       'value' : '1801-12-31T23:59:59Z', "
            + "\n       'results' : { "
            + "\n         'mean' : 3.0, "
            + "\n         'count' : 0 "
            + "\n       } "
            + "\n     } "
            + "\n   ]"
            + "\n } "
            + "\n]}}";

    testAnalytics(analyticsRequest, test);
  }
}

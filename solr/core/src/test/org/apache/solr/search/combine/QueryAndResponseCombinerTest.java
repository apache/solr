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
package org.apache.solr.search.combine;

import java.util.List;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SortedIntDocSet;
import org.junit.Test;

public class QueryAndResponseCombinerTest extends SolrTestCaseJ4 {

  public static List<QueryResult> getQueryResults() {
    QueryResult r1 = new QueryResult();
    r1.setDocList(
        new DocSlice(
            0,
            2,
            new int[] {1, 2},
            new float[] {0.67f, 0, 0.62f},
            3,
            0.67f,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    r1.setDocSet(new SortedIntDocSet(new int[] {1, 2, 3}, 3));
    QueryResult r2 = new QueryResult();
    r2.setDocList(
        new DocSlice(
            0,
            1,
            new int[] {0},
            new float[] {0.87f},
            2,
            0.87f,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    r2.setDocSet(new SortedIntDocSet(new int[] {0, 1}, 2));
    return List.of(r1, r2);
  }

  @Test
  public void simpleCombine() {
    QueryResult queryResult = QueryAndResponseCombiner.simpleCombine(getQueryResults());
    assertEquals(3, queryResult.getDocList().size());
    assertEquals(4, queryResult.getDocSet().size());
  }
}

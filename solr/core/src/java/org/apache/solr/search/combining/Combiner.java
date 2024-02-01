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

package org.apache.solr.search.combining;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.QueryResult;

import static org.apache.solr.common.params.CombinerParams.RECIPROCAl_RANK_FUSION;

/**
 * Combining considers two or more query results: resultA, resultB ...<br>
 * For a given query, each query result is a ranked list of documents La = (a1,a2,...), Lb = (b1,
 * b2, ...)...<br>
 * A combining algorithm creates a unique ranked list I = (i1, i2, ...).<br>
 * This list is created by combining elements from the lists la and lb as described by the
 * implementation algorithm.<br>
 */
public abstract class Combiner {
  
  protected int upTo;

  public Combiner(SolrParams requestParams) {
    this.upTo = requestParams.getInt(
            CombinerParams.COMBINER_UP_TO, CombinerParams.COMBINER_UP_TO_DEFAULT);
  }

  public abstract QueryResult combine(QueryResult[] queryResults);

  QueryResult initCombinedResult(QueryResult[] queryResults) {
    QueryResult combinedResult = new QueryResult();
    boolean partialResults = false;
    for (QueryResult result : queryResults) {
      partialResults |= result.isPartialResults();
    }
    combinedResult.setPartialResults(partialResults);

    boolean segmentTerminatedEarly = false;
    for (QueryResult result : queryResults) {
      if (result.getSegmentTerminatedEarly() != null) {
        segmentTerminatedEarly |= result.getSegmentTerminatedEarly();
      }
    }
    combinedResult.setSegmentTerminatedEarly(segmentTerminatedEarly);

    combinedResult.setNextCursorMark(queryResults[0].getNextCursorMark());
    return combinedResult;
  }
  
  public static Combiner getImplementation(SolrParams requestParams) {
    String algorithm = requestParams.get(
            CombinerParams.COMBINER_ALGORITHM, RECIPROCAl_RANK_FUSION);
    switch (algorithm) {
      case RECIPROCAl_RANK_FUSION:
        return new ReciprocalRankFusion(requestParams);
      default:
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Unknown Combining algorithm: " + algorithm);
    }
  }
}

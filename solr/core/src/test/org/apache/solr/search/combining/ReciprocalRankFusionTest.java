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

import org.apache.lucene.search.ScoreDoc;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;

import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
import static org.apache.solr.common.params.CombinerParams.COMBINER_RRF_K;
import static org.apache.solr.common.params.CombinerParams.COMBINER_UP_TO;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;

public class ReciprocalRankFusionTest extends SolrTestCase {
  private ReciprocalRankFusion toTest;
  
  private QueryResult partial;
  private QueryResult a;
  private QueryResult b;
  private QueryResult c;

  @Before
  public void setup() {
    initQueryResults();
  }

  protected void initQueryResults() {
    int numResults = 10;
    float[] descendingScores = new float[]{10,9,8,7,6,5,4,3,2,1};
    int[] docIdsAPartial = new int[]{7,6,8};
    int[] docIdsA = new int[]{7,6,8,9,3,5,4,2,1,0};
    int[] docIdsB = new int[]{3,9,7,6,8,4,1,0,2,5};
    int[] docIdsC = new int[]{90,70,6,80,5,3,4,2};

    partial = new QueryResult();
    partial.setPartialResults(true);
    DocList aListPartial = new DocSlice(
            0,
            3,
            docIdsAPartial,
            Arrays.copyOfRange(descendingScores, 0, docIdsAPartial.length-1),
            3,
            descendingScores[0],
            GREATER_THAN_OR_EQUAL_TO);
    partial.setDocList(aListPartial);

    a = new QueryResult();
    DocList aList = new DocSlice(
            0,
            numResults,
            docIdsA,
            descendingScores,
            numResults,
            descendingScores[0],
            GREATER_THAN_OR_EQUAL_TO);
    a.setDocList(aList);

    b = new QueryResult();
    DocList bList = new DocSlice(
            0,
            numResults,
            docIdsB,
            descendingScores,
            numResults,
            descendingScores[0],
            GREATER_THAN_OR_EQUAL_TO);
    b.setDocList(bList);

    c = new QueryResult();
    DocList cList = new DocSlice(
            0,
            numResults,
            docIdsC,
            Arrays.copyOfRange(descendingScores, 0, docIdsC.length-1),
            numResults,
            descendingScores[0],
            GREATER_THAN_OR_EQUAL_TO);
    c.setDocList(cList);
  }

  @Test
  public void combining_inputParams_shouldDoCorrectInitialization() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(COMBINER_UP_TO, "77");
    params.add(COMBINER_RRF_K, "111");
    
    toTest = new ReciprocalRankFusion(params);
    
    assertThat(toTest.upTo, is(77));
    assertThat(toTest.k, is(111));
  }
  
  @Test
  public void combining_onePartialResult_shouldReturnPartialResults() {
    int[] expectedDocIds = new int[]{7,6,8,3,9,4,5,1,2,0};
    float[] expectedScores = new float[]{1f/61+1f/61+1f/63,
            1f/62+1f/62+1f/64,
            1f/63+1f/63+1f/65,
            1f/65+1f/61,
            1f/64+1f/62,
            1f/67+1f/66,
            1f/66+1f/70,
            1f/69+1f/67,
            1f/68+1f/69,
            1f/70+1f/68};

    ModifiableSolrParams params = new ModifiableSolrParams();
    
    toTest = new ReciprocalRankFusion(params);
    
    QueryResult[] resultsToCombine = new QueryResult[]{a,b,partial};
    QueryResult combined = toTest.combine(resultsToCombine);

    assertThat(combined.isPartialResults(), is(true));
    DocIterator docs = combined.getDocList().iterator();
    
    int i=0;
    while (docs.hasNext()){
      assertThat(docs.nextDoc(), is(expectedDocIds[i]));
      Assert.assertEquals(expectedScores[i], docs.score(), 0.01f);
      i++;
    }
  }

  @Test
  public void combining_upTo3_shouldCombineOnlyTop3() {
    int[] expectedDocIds = new int[]{7,6,3,90,70,9,8};
    float[] expectedScores = new float[]{1f/61+1f/63,
            1f/62+1f/63,
            1f/61,
            1f/61,
            1f/62,
            1f/62,
            1f/63};

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(COMBINER_UP_TO, "3");
    
    toTest = new ReciprocalRankFusion(params);

    QueryResult[] resultsToCombine = new QueryResult[]{a,b,c};
    QueryResult combined = toTest.combine(resultsToCombine);

    assertThat(combined.isPartialResults(), is(false));
    DocIterator docs = combined.getDocList().iterator();

    int i=0;
    while (docs.hasNext()){
      assertThat(docs.nextDoc(), is(expectedDocIds[i]));
      Assert.assertEquals(expectedScores[i], docs.score(), 0.01f);
      i++;
    }
  }

  @Test
  public void combining_noExplicitUpTo_shouldCombineUpTo100() {//TO DO
    int[] expectedDocIds = new int[]{7,6,8,3,9,4,5,1,2,0};
    float[] expectedScores = new float[]{1f/61+1f/61+1f/63,
            1f/62+1f/62+1f/64,
            1f/63+1f/63+1f/65,
            1f/65+1f/61,
            1f/64+1f/62,
            1f/67+1f/66,
            1f/66+1f/70,
            1f/69+1f/67,
            1f/68+1f/69,
            1f/70+1f/68};

    ModifiableSolrParams params = new ModifiableSolrParams();

    toTest = new ReciprocalRankFusion(params);

    QueryResult[] resultsToCombine = new QueryResult[]{a,b,partial};
    QueryResult combined = toTest.combine(resultsToCombine);

    assertThat(combined.isPartialResults(), is(true));
    DocIterator docs = combined.getDocList().iterator();

    int i=0;
    while (docs.hasNext()){
      assertThat(docs.nextDoc(), is(expectedDocIds[i]));
      Assert.assertEquals(expectedScores[i], docs.score(), 0.01f);
      i++;
    }
  }

  @Test
  public void combining_kParameter_shouldAffectScoring() {//TO DO
    int[] expectedDocIds = new int[]{7,6,8,3,9,4,5,1,2,0};
    float[] expectedScores = new float[]{1f/61+1f/61+1f/63,
            1f/62+1f/62+1f/64,
            1f/63+1f/63+1f/65,
            1f/65+1f/61,
            1f/64+1f/62,
            1f/67+1f/66,
            1f/66+1f/70,
            1f/69+1f/67,
            1f/68+1f/69,
            1f/70+1f/68};

    ModifiableSolrParams params = new ModifiableSolrParams();

    toTest = new ReciprocalRankFusion(params);

    QueryResult[] resultsToCombine = new QueryResult[]{a,b,partial};
    QueryResult combined = toTest.combine(resultsToCombine);

    assertThat(combined.isPartialResults(), is(true));
    DocIterator docs = combined.getDocList().iterator();

    int i=0;
    while (docs.hasNext()){
      assertThat(docs.nextDoc(), is(expectedDocIds[i]));
      Assert.assertEquals(expectedScores[i], docs.score(), 0.01f);
      i++;
    }
  }
}

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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.ScoreDoc;

public class ReRankScaler {

  protected int mainQueryMin = -1;
  protected int mainQueryMax = -1;
  protected int reRankQueryMin = -1;
  protected int reRankQueryMax = -1;
  protected ReRankOperator reRankOperator;

  public ReRankScaler(String mainRange, String reRankRange, ReRankOperator reRankOperator)
      throws SyntaxError {

    if (reRankOperator != ReRankOperator.ADD
        && reRankOperator != ReRankOperator.MULTIPLY
        && reRankOperator != ReRankOperator.REPLACE) {
      throw new SyntaxError("ReRank scaling only supports ADD, Multiply and Replace operators");
    } else {
      this.reRankOperator = reRankOperator;
    }

    if (mainRange != null) {
      String[] mainMinMax = mainRange.split("-");
      this.mainQueryMin = Integer.parseInt(mainMinMax[0]);
      this.mainQueryMax = Integer.parseInt(mainMinMax[1]);
    }

    if (reRankRange != null) {
      String[] reRankMinMax = reRankRange.split("-");
      this.reRankQueryMin = Integer.parseInt(reRankMinMax[0]);
      this.reRankQueryMax = Integer.parseInt(reRankMinMax[1]);
    }
  }

  public int getMainQueryMin() {
    return mainQueryMin;
  }

  public int getMainQueryMax() {
    return mainQueryMax;
  }

  public int getReRankQueryMin() {
    return reRankQueryMin;
  }

  public int getReRankQueryMax() {
    return reRankQueryMax;
  }

  public boolean scaleScores() {
    if (scaleMainScores() || scaleReRankScores()) {
      return true;
    } else {
      return false;
    }
  }

  public boolean scaleMainScores() {
    if (mainQueryMin > -1 && mainQueryMax > -1) {
      return true;
    } else {
      return false;
    }
  }

  public boolean scaleReRankScores() {
    if (reRankQueryMin > -1 && reRankQueryMax > -1) {
      return true;
    } else {
      return false;
    }
  }

  public ScoreDoc[] scaleScores(ScoreDoc[] originalDocs, ScoreDoc[] rescoredDocs, int howMany) {

    Map<Integer, Float> originalScoreMap = new HashMap<>();
    Map<Integer, Float> rescoredMap = new HashMap<>();

    Map<Integer, Float> scaledOriginalScoreMap = null;
    Map<Integer, Float> scaledRescoredMap = null;

    for (ScoreDoc scoreDoc : originalDocs) {
      originalScoreMap.put(scoreDoc.doc, scoreDoc.score);
    }

    if (scaleMainScores()) {
      scaledOriginalScoreMap = minMaxScaleScores(originalScoreMap, mainQueryMin, mainQueryMax);
    } else {
      scaledOriginalScoreMap = originalScoreMap;
    }

    // System.out.println("HOW MANY:"+howMany);
    for (int i = 0; i < howMany; i++) {
      ScoreDoc rescoredDoc = rescoredDocs[i];
      int doc = rescoredDoc.doc;
      float score = rescoredDoc.score;
      float rescore = getReRankScore(originalScoreMap.get(doc), score, reRankOperator);
      // System.out.println("RESCORE:"+rescore);
      if (rescore > 0) {
        rescoredMap.put(doc, rescore);
      }
    }

    if (scaleReRankScores()) {
      scaledRescoredMap = minMaxScaleScores(rescoredMap, reRankQueryMin, reRankQueryMax);
    } else {
      scaledRescoredMap = rescoredMap;
    }

    ScoreDoc[] scaledScoreDocs = new ScoreDoc[originalDocs.length];
    int index = 0;
    for (Map.Entry<Integer, Float> entry : scaledOriginalScoreMap.entrySet()) {
      int doc = entry.getKey();
      float scaledScore = entry.getValue();
      ScoreDoc scoreDoc = null;
      if (scaledRescoredMap.containsKey(doc)) {
        switch (reRankOperator) {
          case ADD:
            scoreDoc = new ScoreDoc(doc, scaledScore + scaledRescoredMap.get(doc));
            break;
          case MULTIPLY:
            scoreDoc = new ScoreDoc(doc, scaledScore * scaledRescoredMap.get(doc));
            break;
          case REPLACE:
            scoreDoc = new ScoreDoc(doc, scaledRescoredMap.get(doc));
            break;
        }
      } else {
        scoreDoc = new ScoreDoc(doc, scaledScore);
      }

      scaledScoreDocs[index++] = scoreDoc;
    }

    Comparator<ScoreDoc> sortDocComparator =
        new Comparator<ScoreDoc>() {
          @Override
          public int compare(ScoreDoc a, ScoreDoc b) {
            // Sort by score descending, then docID ascending:
            if (a.score > b.score) {
              return -1;
            } else if (a.score < b.score) {
              return 1;
            } else {
              // This subtraction can't overflow int
              // because docIDs are >= 0:
              return a.doc - b.doc;
            }
          }
        };

    Arrays.sort(scaledScoreDocs, sortDocComparator);
    return scaledScoreDocs;
  }

  public static float getReRankScore(
      float originalScore, float combinedScore, ReRankOperator reRankOperator) {
    // System.out.println("Orignal and Combined:"+originalScore+" : "+combinedScore+" :
    // "+reRankOperator.toString());
    if (originalScore == combinedScore) {
      return 0;
    }
    switch (reRankOperator) {
      case ADD:
        return combinedScore - originalScore;
      case REPLACE:
        return combinedScore;
      case MULTIPLY:
        return combinedScore / originalScore;
      default:
        return -1;
    }
  }

  public static Map<Integer, Float> minMaxScaleScores(
      Map<Integer, Float> docScoreMap, float min, float max) {
    // System.out.println("SCALING:"+docScoreMap);
    // System.out.println("BETWEEN:"+min+" : "+ max);
    Map<Integer, Float> scaledScores = new HashMap<>();
    float localMin = Float.MAX_VALUE;
    float localMax = Float.MIN_VALUE;

    for (float score : docScoreMap.values()) {
      // System.out.println("SCORE/MIN/MAX: "+score+"/"+localMin+"/"+localMax);
      localMin = Math.min(localMin, score);
      localMax = Math.max(localMax, score);
    }

    if (localMin == localMax) {
      // All the scores are the same set the halfway between min and max
      float halfway = (min + max) / 2;
      for (Map.Entry<Integer, Float> entry : docScoreMap.entrySet()) {
        int doc = entry.getKey();
        scaledScores.put(doc, halfway);
      }
      return scaledScores;
    }

    // System.out.println("LOCAL MIN/MAX"+localMin+":"+localMax);
    for (Map.Entry<Integer, Float> entry : docScoreMap.entrySet()) {
      int doc = entry.getKey();
      float score = entry.getValue();
      float scaled = (score - localMin) / (localMax - localMin);
      scaledScores.put(doc, scaled);
    }

    if (min != 0 || max != 1) {
      // Next scale between specific min/max
      float scale = max - min;

      for (Map.Entry<Integer, Float> entry : scaledScores.entrySet()) {
        float score = entry.getValue();
        entry.setValue((scale * score) + min);
      }
    }

    // System.out.println("SCALED:"+scaledScores);

    return scaledScores;
  }
}

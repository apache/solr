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
import org.apache.solr.request.SolrRequestInfo;

public class ReRankScaler {

  protected int mainQueryMin = -1;
  protected int mainQueryMax = -1;
  protected int reRankQueryMin = -1;
  protected int reRankQueryMax = -1;
  protected ReRankOperator reRankOperator;
  protected ReRankScalerExplain reRankScalerExplain;

  public ReRankScaler(String mainScale, String reRankScale, ReRankOperator reRankOperator)
      throws SyntaxError {

    this.reRankScalerExplain = new ReRankScalerExplain(mainScale, reRankScale);
    if (reRankOperator != ReRankOperator.ADD
        && reRankOperator != ReRankOperator.MULTIPLY
        && reRankOperator != ReRankOperator.REPLACE) {
      throw new SyntaxError("ReRank scaling only supports ADD, Multiply and Replace operators");
    } else {
      this.reRankOperator = reRankOperator;
    }

    if (reRankScalerExplain.getMainScale() != null) {
      String[] mainMinMax = reRankScalerExplain.getMainScale().split("-");
      this.mainQueryMin = Integer.parseInt(mainMinMax[0]);
      this.mainQueryMax = Integer.parseInt(mainMinMax[1]);
    }

    if (reRankScalerExplain.getReRankScale() != null) {
      String[] reRankMinMax = reRankScalerExplain.getReRankScale().split("-");
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

  public ReRankScalerExplain getReRankScalerExplain() {
    return this.reRankScalerExplain;
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
      MinMaxExplain mainExplain = getMinMaxExplain(mainQueryMin, mainQueryMax, originalScoreMap);
      scaledOriginalScoreMap = minMaxScaleScores(originalScoreMap, mainExplain);
      System.out.println("Scaled Main Scores:" + scaledOriginalScoreMap);
      SolrRequestInfo.getRequestInfo().getResponseBuilder().mainScaleExplain = mainExplain;
      reRankScalerExplain.setMainScaleExplain(mainExplain);
    } else {
      scaledOriginalScoreMap = originalScoreMap;
    }

    for (int i = 0; i < howMany; i++) {
      ScoreDoc rescoredDoc = rescoredDocs[i];
      int doc = rescoredDoc.doc;
      float score = rescoredDoc.score;
      float rescore = getReRankScore(originalScoreMap.get(doc), score, reRankOperator);
      if (rescore > 0) {
        rescoredMap.put(doc, rescore);
      }
    }

    if (scaleReRankScores()) {
      MinMaxExplain reRankExplain = getMinMaxExplain(reRankQueryMin, reRankQueryMax, rescoredMap);
      scaledRescoredMap = minMaxScaleScores(rescoredMap, reRankExplain);
      System.out.println("Scaled reRank Scores:" + scaledRescoredMap);
      SolrRequestInfo.getRequestInfo().getResponseBuilder().reRankScaleExplain = reRankExplain;
      reRankScalerExplain.setReRankScaleExplain(reRankExplain);
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

  public static float combineScores(
      float orginalScore, float reRankScore, ReRankOperator reRankOperator) {
    switch (reRankOperator) {
      case ADD:
        return orginalScore + reRankScore;
      case REPLACE:
        return reRankScore;
      case MULTIPLY:
        return orginalScore * reRankScore;
      default:
        return -1;
    }
  }

  public static final class ReRankScalerExplain {
    private MinMaxExplain mainScaleExplain;
    private MinMaxExplain reRankScaleExplain;
    private String mainScale;
    private String reRankScale;

    public ReRankScalerExplain(String mainScale, String reRankScale) {
      this.mainScale = mainScale;
      this.reRankScale = reRankScale;
    }

    public MinMaxExplain getMainScaleExplain() {
      return mainScaleExplain;
    }

    public MinMaxExplain getReRankScaleExplain() {
      return reRankScaleExplain;
    }

    public void setMainScaleExplain(MinMaxExplain mainScaleExplain) {
      this.mainScaleExplain = mainScaleExplain;
    }

    public void setReRankScaleExplain(MinMaxExplain reRankScaleExplain) {
      this.reRankScaleExplain = reRankScaleExplain;
    }

    public boolean reRankScale() {
      return (getMainScale() != null || getReRankScale() != null);
    }

    public String getMainScale() {
      return this.mainScale;
    }

    public String getReRankScale() {
      return this.reRankScale;
    }
  }

  public static final class MinMaxExplain {
    public final float scaleMin;
    public final float scaleMax;
    public final float localMin;
    public final float localMax;
    private float diff;

    public MinMaxExplain(float scaleMin, float scaleMax, float localMin, float localMax) {
      this.scaleMin = scaleMin;
      this.scaleMax = scaleMax;
      this.localMin = localMin;
      this.localMax = localMax;
      this.diff = scaleMax - scaleMin;
    }

    public float scale(float score) {
      if (localMin == localMax) {
        return (scaleMin + scaleMax) / 2;
      } else {
        float scaledScore = (score - localMin) / (localMax - localMin);
        if (scaleMin != 0 || scaleMax != 1) {
          scaledScore = (diff * scaledScore) + scaleMin;
          return scaledScore;
        } else {
          return scaledScore;
        }
      }
    }
  }

  public static MinMaxExplain getMinMaxExplain(
      float scaleMin, float scaleMax, Map<Integer, Float> docScoreMap) {
    float localMin = Float.MAX_VALUE;
    float localMax = Float.MIN_VALUE;

    for (float score : docScoreMap.values()) {
      localMin = Math.min(localMin, score);
      localMax = Math.max(localMax, score);
    }
    return new MinMaxExplain(scaleMin, scaleMax, localMin, localMax);
  }

  public static Map<Integer, Float> minMaxScaleScores(
      Map<Integer, Float> docScoreMap, MinMaxExplain explain) {

    Map<Integer, Float> scaledScores = new HashMap<>();
    for (Map.Entry<Integer, Float> entry : docScoreMap.entrySet()) {
      int doc = entry.getKey();
      float score = entry.getValue();
      scaledScores.put(doc, explain.scale(score));
    }

    return scaledScores;
  }
}

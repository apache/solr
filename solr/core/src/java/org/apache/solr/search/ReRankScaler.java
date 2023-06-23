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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.QueryRescorer;
import org.apache.lucene.search.ScoreDoc;

public class ReRankScaler {

  protected int mainQueryMin = -1;
  protected int mainQueryMax = -1;
  protected int reRankQueryMin = -1;
  protected int reRankQueryMax = -1;
  protected boolean debugQuery;
  protected ReRankOperator reRankOperator;
  protected ReRankScalerExplain reRankScalerExplain;
  private QueryRescorer replaceRescorer;
  private Set<Integer> reRankSet;
  private double reRankScaleWeight;

  public ReRankScaler(
      String mainScale,
      String reRankScale,
      double reRankScaleWeight,
      ReRankOperator reRankOperator,
      QueryRescorer replaceRescorer,
      boolean debugQuery)
      throws SyntaxError {

    this.reRankScaleWeight = reRankScaleWeight;
    this.debugQuery = debugQuery;
    this.reRankScalerExplain = new ReRankScalerExplain(mainScale, reRankScale);
    this.replaceRescorer = replaceRescorer;
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

  @Override
  public int hashCode() {
    return Integer.hashCode(mainQueryMax)
        + Integer.hashCode(mainQueryMin)
        + Integer.hashCode(reRankQueryMin)
        + Integer.hashCode(reRankQueryMax)
        + reRankOperator.toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ReRankScaler) {
      ReRankScaler _reRankScaler = (ReRankScaler) o;
      if (mainQueryMin == _reRankScaler.mainQueryMin
          && mainQueryMax == _reRankScaler.mainQueryMax
          && reRankQueryMin == _reRankScaler.reRankQueryMin
          && reRankQueryMax == _reRankScaler.reRankQueryMax
          && reRankOperator.equals(_reRankScaler.reRankOperator)) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  public QueryRescorer getReplaceRescorer() {
    return replaceRescorer;
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

  public double getReRankScaleWeight() {
    return this.reRankScaleWeight;
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
    Map<Integer, Float> scaledOriginalScoreMap = null;
    Map<Integer, Float> scaledRescoredMap = null;
    Map<Integer, Float> rescoredMap = new HashMap<>();

    for (ScoreDoc scoreDoc : originalDocs) {
      originalScoreMap.put(scoreDoc.doc, scoreDoc.score);
    }

    if (scaleMainScores()) {
      MinMaxExplain mainExplain = getMinMaxExplain(mainQueryMin, mainQueryMax, originalScoreMap);
      scaledOriginalScoreMap = minMaxScaleScores(originalScoreMap, mainExplain);
      reRankScalerExplain.setMainScaleExplain(mainExplain);
    } else {
      scaledOriginalScoreMap = originalScoreMap;
    }

    this.reRankSet = debugQuery ? new HashSet<>() : null;

    for (int i = 0; i < howMany; i++) {
      ScoreDoc rescoredDoc = rescoredDocs[i];
      int doc = rescoredDoc.doc;
      if (debugQuery) {
        reRankSet.add(doc);
      }
      float score = rescoredDoc.score;
      if (score > 0) {
        rescoredMap.put(doc, score);
      }
    }

    if (scaleReRankScores()) {
      MinMaxExplain reRankExplain = getMinMaxExplain(reRankQueryMin, reRankQueryMax, rescoredMap);
      scaledRescoredMap = minMaxScaleScores(rescoredMap, reRankExplain);
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
        scoreDoc =
            new ScoreDoc(
                doc,
                combineScores(
                    scaledScore, scaledRescoredMap.get(doc), reRankScaleWeight, reRankOperator));
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

  public static float combineScores(
      float orginalScore,
      float reRankScore,
      double reRankScaleWeight,
      ReRankOperator reRankOperator) {
    switch (reRankOperator) {
      case ADD:
        return (float) (orginalScore + reRankScaleWeight * reRankScore);
      case REPLACE:
        return (float) (reRankScaleWeight * reRankScore);
      case MULTIPLY:
        return (float) (orginalScore * reRankScaleWeight * reRankScore);
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

    public Explanation explain() {
      if (getMainScale() != null && getReRankScale() != null) {
        return Explanation.noMatch(
            "ReRankScaler Explain",
            mainScaleExplain.explain("first pass scale"),
            reRankScaleExplain.explain("second pass scale"));
      } else if (getMainScale() != null) {
        return mainScaleExplain.explain("first pass scale");
      } else if (getReRankScale() != null) {
        return reRankScaleExplain.explain("second pass scale");
      }
      return null;
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

    public Explanation explain(String message) {
      return Explanation.noMatch(
          message,
          Explanation.match(localMin, "min score"),
          Explanation.match(localMax, "max score"));
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

  public Explanation explain(
      int doc, Explanation mainQueryExplain, Explanation reRankQueryExplain) {
    float reRankScore = reRankQueryExplain.getDetails()[1].getValue().floatValue();
    float mainScore = mainQueryExplain.getValue().floatValue();
    if (reRankSet.contains(doc)) {
      if (scaleMainScores() && scaleReRankScores()) {
        if (reRankScore > 0) {
          MinMaxExplain mainScaleExplain = reRankScalerExplain.getMainScaleExplain();
          MinMaxExplain reRankScaleExplain = reRankScalerExplain.getReRankScaleExplain();
          float scaledMainScore = mainScaleExplain.scale(mainScore);
          float scaledReRankScore = reRankScaleExplain.scale(reRankScore);
          float combinedScaleScore =
              combineScores(scaledMainScore, scaledReRankScore, reRankScaleWeight, reRankOperator);

          return Explanation.match(
              combinedScaleScore,
              "combined scaled first and second pass score",
              Explanation.match(
                  scaledMainScore,
                  "first pass score scaled between: " + reRankScalerExplain.getMainScale(),
                  reRankQueryExplain.getDetails()[0],
                  Explanation.match(mainScaleExplain.localMin, "min first pass score"),
                  Explanation.match(mainScaleExplain.localMax, "max first pass score")),
              Explanation.match(
                  scaledReRankScore,
                  "second pass score scaled between: " + reRankScalerExplain.getReRankScale(),
                  reRankQueryExplain.getDetails()[1],
                  Explanation.match(reRankScaleExplain.localMin, "min second pass score"),
                  Explanation.match(reRankScaleExplain.localMax, "max second pass score")),
              Explanation.match(reRankScaleWeight, "rerank weight"));

        } else {
          MinMaxExplain mainScaleExplain = reRankScalerExplain.getMainScaleExplain();
          float scaledMainScore = mainScaleExplain.scale(mainScore);
          return Explanation.match(
              scaledMainScore,
              "combined scaled first and second pass score",
              Explanation.match(
                  scaledMainScore,
                  "scaled first pass score",
                  reRankQueryExplain.getDetails()[0],
                  Explanation.match(mainScaleExplain.localMin, "min first pass score"),
                  Explanation.match(mainScaleExplain.localMax, "max first pass score")),
              reRankQueryExplain.getDetails()[1]);
        }
      } else if (scaleMainScores() && !scaleReRankScores()) {
        MinMaxExplain mainScaleExplain = reRankScalerExplain.getMainScaleExplain();
        float scaledMainScore = mainScaleExplain.scale(mainScore);
        float combinedScaleScore =
            combineScores(scaledMainScore, reRankScore, reRankScaleWeight, reRankOperator);
        return Explanation.match(
            combinedScaleScore,
            "combined scaled first and unscaled second pass score ",
            Explanation.match(
                scaledMainScore,
                "first pass score scaled between: " + reRankScalerExplain.getMainScale(),
                reRankQueryExplain.getDetails()[0],
                Explanation.match(mainScaleExplain.localMin, "min first pass score"),
                Explanation.match(mainScaleExplain.localMax, "max first pass score")),
            reRankQueryExplain.getDetails()[1],
            Explanation.match(reRankScaleWeight, "rerank weight"));

      } else if (!scaleMainScores() && scaleReRankScores()) {
        if (reRankScore > 0) {
          MinMaxExplain reRankScaleExplain = reRankScalerExplain.getReRankScaleExplain();
          float scaledReRankScore = reRankScaleExplain.scale(reRankScore);
          float combinedScaleScore =
              combineScores(mainScore, scaledReRankScore, reRankScaleWeight, reRankOperator);
          return Explanation.match(
              combinedScaleScore,
              "combined unscaled first and scaled second pass score ",
              reRankQueryExplain.getDetails()[0],
              Explanation.match(
                  scaledReRankScore,
                  "second pass score scaled between:" + reRankScalerExplain.reRankScale,
                  reRankQueryExplain.getDetails()[1],
                  Explanation.match(reRankScaleExplain.localMin, "min second pass score"),
                  Explanation.match(reRankScaleExplain.localMax, "max sceond pass score")),
              Explanation.match(reRankScaleWeight, "rerank weight"));
        } else {
          return null;
        }
      } else {
        // If we get here nothing has been scaled so return null
        return null;
      }
    } else {
      if (scaleMainScores()) {
        MinMaxExplain mainScaleExplain = reRankScalerExplain.getMainScaleExplain();
        float scaledMainScore = mainScaleExplain.scale(mainScore);
        return Explanation.match(
            scaledMainScore,
            "scaled main query score between: " + reRankScalerExplain.mainScale,
            mainQueryExplain,
            Explanation.match(mainScaleExplain.localMin, "min main query score"),
            Explanation.match(mainScaleExplain.localMax, "max main query score"));
      } else {
        return null;
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

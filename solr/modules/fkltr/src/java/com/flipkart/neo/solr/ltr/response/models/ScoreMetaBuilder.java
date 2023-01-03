package com.flipkart.neo.solr.ltr.response.models;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.flipkart.ad.selector.api.Score;
import com.flipkart.ad.selector.model.supply.PlacementInfo;
import com.flipkart.neo.solr.ltr.banner.entity.HitBanner;
import org.apache.commons.collections4.MapUtils;

public class ScoreMetaBuilder {

  private ScoreMetaBuilder(){}

  public static ScoreMeta buildScoreMeta(HitBanner banner) {
    ScoreMeta scoreMeta = new ScoreMeta();
    scoreMeta.setScoreSplit(adaptScoreSplit(banner.getScore()));
    scoreMeta.setModelInfoMap(adaptModelInfoMap(banner.getModelTypeToModelInfo()));
    scoreMeta.setRelevanceType(banner.getRelevanceType());
    scoreMeta.setExplored(banner.isExplore());
    scoreMeta.setPlacementScoresSet(getPlacementScores(banner));
    return scoreMeta;
  }

  private static Map<Score.Dimension, Double> adaptScoreSplit(Map<Score.Dimension, Score> scores) {
    if (scores == null) return null;
    Map<Score.Dimension, Double> scoreSplit = new EnumMap<>(Score.Dimension.class);
    for (Map.Entry<Score.Dimension, Score> scoreEntry : scores.entrySet()) {
      scoreSplit.put(scoreEntry.getKey(), scoreEntry.getValue().getValue());
    }
    return  scoreSplit;
  }

  private static Map<Score.Dimension, ModelInfo> adaptModelInfoMap(
      Map<Score.Dimension, com.flipkart.neo.selector.ModelInfo> modelTypeToModelInfo
  ) {
    if (modelTypeToModelInfo == null) return null;
    Map<Score.Dimension, ModelInfo> modelInfoMap = new EnumMap<>(Score.Dimension.class);
    for (Map.Entry<Score.Dimension, com.flipkart.neo.selector.ModelInfo> entry : modelTypeToModelInfo.entrySet()) {
      modelInfoMap.put(entry.getKey(), adaptModelInfo(entry.getValue()));
    }

    return modelInfoMap;
  }

  private static ModelInfo adaptModelInfo(
      com.flipkart.neo.selector.ModelInfo input) {
    ModelInfo modelInfo = new ModelInfo();
    modelInfo.setModelName(input.getModelName());
    modelInfo.setModelVersion(input.getModelVersion());

    return modelInfo;
  }

  private static Set<PlacementScore> getPlacementScores(HitBanner banner) {
    if(MapUtils.isEmpty(banner.getPlacementScoreMap())) {
        return null;
    }

    Set<PlacementScore> placementScores = new HashSet<>();
    for(PlacementInfo placementInfo : banner.getPlacementScoreMap().keySet()) {
      placementScores.add(new PlacementScore(placementInfo, banner.getPlacementScoreMap().get(placementInfo)));
    }
    return placementScores;
  }
}

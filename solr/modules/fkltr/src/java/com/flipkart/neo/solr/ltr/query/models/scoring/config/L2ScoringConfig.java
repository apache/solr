package com.flipkart.neo.solr.ltr.query.models.scoring.config;

import com.flipkart.ad.selector.api.Score;
import com.flipkart.ad.selector.model.allocation.AggregateScorerType;

import java.util.Set;

public class L2ScoringConfig {
  // scorers to be applied.
  private Set<Score.Dimension> scorers;
  // aggregation type.
  private AggregateScorerType aggregateScorerType;

  // corresponding configs for applicable scorers.
  private ModelScoringConfig pctrScoringConfig;
  private ModelScoringConfig pcvrScoringConfig;
  private CommonScoringConfig priceScoringConfig;
  private CommonScoringConfig aspScoringConfig;
  private ViewScoringConfig viewScoringConfig;
  private DiversityScoringConfig diversityScoringConfig;
  private ContextualContentScoringConfig contextualContentScoringConfig;

  public Set<Score.Dimension> getScorers() {
    return scorers;
  }

  public void setScorers(Set<Score.Dimension> scorers) {
    this.scorers = scorers;
  }

  public AggregateScorerType getAggregateScorerType() {
    return aggregateScorerType;
  }

  public void setAggregateScorerType(AggregateScorerType aggregateScorerType) {
    this.aggregateScorerType = aggregateScorerType;
  }

  public ModelScoringConfig getPctrScoringConfig() {
    return pctrScoringConfig;
  }

  public void setPctrScoringConfig(ModelScoringConfig pctrScoringConfig) {
    this.pctrScoringConfig = pctrScoringConfig;
  }

  public ModelScoringConfig getPcvrScoringConfig() {
    return pcvrScoringConfig;
  }

  public void setPcvrScoringConfig(ModelScoringConfig pcvrScoringConfig) {
    this.pcvrScoringConfig = pcvrScoringConfig;
  }

  public CommonScoringConfig getPriceScoringConfig() {
    return priceScoringConfig;
  }

  public void setPriceScoringConfig(CommonScoringConfig priceScoringConfig) {
    this.priceScoringConfig = priceScoringConfig;
  }

  public CommonScoringConfig getAspScoringConfig() {
    return aspScoringConfig;
  }

  public void setAspScoringConfig(CommonScoringConfig aspScoringConfig) {
    this.aspScoringConfig = aspScoringConfig;
  }

  public ViewScoringConfig getViewScoringConfig() {
    return viewScoringConfig;
  }

  public void setViewScoringConfig(ViewScoringConfig viewScoringConfig) {
    this.viewScoringConfig = viewScoringConfig;
  }

  public ContextualContentScoringConfig getContextualContentScoringConfig() {
    return contextualContentScoringConfig;
  }

  public void setContextualContentScoringConfig(ContextualContentScoringConfig contextualContentScoringConfig) {
    this.contextualContentScoringConfig = contextualContentScoringConfig;
  }

  public DiversityScoringConfig getDiversityScoringConfig() {
    return diversityScoringConfig;
  }

  public void setDiversityScoringConfig(DiversityScoringConfig diversityScoringConfig) {
    this.diversityScoringConfig = diversityScoringConfig;
  }
}
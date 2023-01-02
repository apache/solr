package com.flipkart.neo.solr.ltr.search;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.query.models.RescoringContext;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.neo.solr.ltr.query.models.ScoringScheme;
import com.flipkart.solr.ltr.scorer.FKLTRDocumentScorer;
import com.flipkart.solr.ltr.search.FKLTRQParserPlugin;
import org.apache.solr.common.SolrException;

public class NeoLTRQParserPlugin extends FKLTRQParserPlugin<NeoRescoringContext, ScoreMeta> {
  private ObjectMapper mapper;
  private FKLTRDocumentScorer<NeoRescoringContext, ScoreMeta> fkltrDocumentScorer;

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public void setFkltrDocumentScorer(FKLTRDocumentScorer<NeoRescoringContext, ScoreMeta> fkltrDocumentScorer) {
    this.fkltrDocumentScorer = fkltrDocumentScorer;
  }

  @Override
  protected FKLTRDocumentScorer<NeoRescoringContext, ScoreMeta> provideFKLTRDocumentScorer() {
    return fkltrDocumentScorer;
  }

  @Override
  protected NeoRescoringContext parseRescoringContext(String contextString) {
    RescoringContext rescoringContext;
    try {
      rescoringContext = mapper.readValue(contextString, RescoringContext.class);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "unable to parse RescoringContext, " + e.getMessage());
    }

    validateRescoringContext(rescoringContext);

    return adapt(rescoringContext);
  }

  private void validateRescoringContext(RescoringContext rescoringContext) {
    if (rescoringContext.getScoringSchemeConfig() == null ||
        rescoringContext.getScoringSchemeConfig().getScoringScheme() == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Must specify scoringScheme");
    }

    if (rescoringContext.getScoringSchemeConfig().getScoringScheme() == ScoringScheme.L1_GROUP_L2 ||
        rescoringContext.getScoringSchemeConfig().getScoringScheme() == ScoringScheme.L1_GROUP_LIMIT) {
      if (rescoringContext.getScoringSchemeConfig().getGroupField() == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "requires filed to group on");
      }

      if (rescoringContext.getScoringSchemeConfig().getGroupsRequiredWithLimits() == null ||
          rescoringContext.getScoringSchemeConfig().getGroupsRequiredWithLimits().isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "requires groups to work upon");
      }
    } else if (rescoringContext.getScoringSchemeConfig().getScoringScheme() == ScoringScheme.L1_LIMIT ||
        rescoringContext.getScoringSchemeConfig().getScoringScheme() == ScoringScheme.L1_L2) {
      if (rescoringContext.getScoringSchemeConfig().getL2Limit() <= 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "requires at least 1 document for l2");
      }
    }
  }

  private NeoRescoringContext adapt(RescoringContext rescoringContext) {
    NeoRescoringContext neoRescoringContext = new NeoRescoringContext();
    neoRescoringContext.setScoringSchemeConfig(rescoringContext.getScoringSchemeConfig());
    neoRescoringContext.setRequestId(rescoringContext.getRequestId());
    neoRescoringContext.setRequestTimeStampInMillis(rescoringContext.getRequestTimeStampInMillis());
    neoRescoringContext.setUserId(rescoringContext.getUserId());
    neoRescoringContext.setVernacularInfo(rescoringContext.getVernacularInfo());
    neoRescoringContext.setL1ScoringConfig(rescoringContext.getL1ScoringConfig());
    neoRescoringContext.setL2ScoringConfig(rescoringContext.getL2ScoringConfig());
    neoRescoringContext.setUserInfo(rescoringContext.getUserInfo());
    neoRescoringContext.setSupplyInfo(rescoringContext.getSupplyInfo());
    neoRescoringContext.setIntentInfo(rescoringContext.getIntentInfo());
    neoRescoringContext.setDebugInfoRequired(rescoringContext.isDebugInfoRequired());
    neoRescoringContext.setPlacementInfos(rescoringContext.getPlacementInfos());
    neoRescoringContext.setUserSegments(rescoringContext.getUserSegments());
    neoRescoringContext.setSegmentFilterRequired(rescoringContext.getSegmentFilterRequired());
    return neoRescoringContext;
  }
}

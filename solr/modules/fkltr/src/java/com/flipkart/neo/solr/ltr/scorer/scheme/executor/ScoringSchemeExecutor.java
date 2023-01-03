package com.flipkart.neo.solr.ltr.scorer.scheme.executor;

import java.util.Collection;

import com.flipkart.neo.solr.ltr.banner.entity.HitBanner;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.neo.solr.ltr.configs.NeoFKLTRConfigs;
import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;
import org.apache.lucene.search.ScoreDoc;

public abstract class ScoringSchemeExecutor {
  protected static final String L1_GROUP_AND_LIMIT = "l1GroupAndLimit";
  protected static final String L1_LIMIT = "l1Limit";
  protected static final String L2_RANKING_SIZE = "l2RankingSize";
  protected static final String L2_RANKING = "l2Ranking";
  protected static final String L2_GROUP_COLLECTION = "l2GroupCollection";
  protected static final String L2_COLLECTION = "l2Collection";

  protected final NeoFKLTRConfigs neoFKLTRConfigs;

  protected ScoringSchemeExecutor(NeoFKLTRConfigs neoFKLTRConfigs) {
    this.neoFKLTRConfigs = neoFKLTRConfigs;
  }


  /**
   * @param hits hits
   * @param rescoringContext rescoringContext
   * @param scoreMetaHolder scoreMetaHolder
   * @return executes corresponding scheme and returns re-sorted docs.
   */
  public abstract ScoreDoc[] executeAndReturnSortedDocs(ScoreDoc[] hits, NeoRescoringContext rescoringContext,
                                                        ScoreMetaHolder<ScoreMeta> scoreMetaHolder);

  protected Collection<HitBanner> l2Rank(BannerScorer bannerScorer, Collection<HitBanner> banners,
                        NeoRescoringContext rescoringContext, ScoreMetaHolder<ScoreMeta> scoreMetaHolder) {
    rescoringContext.setAggregateScoreRequired(isAggregateScoreRequired());

    banners = bannerScorer.l2Score(banners, rescoringContext);

    banners.forEach(banner -> banner.addScoreMeta(scoreMetaHolder));

    if (rescoringContext.isAggregateScoreRequired()) {
      // updating scoreDoc score with aggregate score as it is final score.
      banners.forEach(HitBanner::updateHitScoreWithAggregate);
    }

    return banners;
  }

  /**
   * @param banners input banners.
   * @return corresponding scoreDocs.
   */
  protected ScoreDoc[] collectScoreDocsFromBanners(Collection<HitBanner> banners) {
    ScoreDoc[] scoreDocs = new ScoreDoc[banners.size()];
    int i = 0;
    for (HitBanner banner: banners) {
      scoreDocs[i++] = banner.getHit();
    }
    return scoreDocs;
  }

  private boolean isAggregateScoreRequired() {
    // AggregateScore is only required in multiShard scenario where it will be used as criteria for merging banners
    // across shards.
    return neoFKLTRConfigs.isIsMultiShardSetup();
  }
}

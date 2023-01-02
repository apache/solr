package com.flipkart.neo.solr.ltr.schema.parser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.solr.ltr.banner.entity.HitBanner;
import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;

@Ignore
public abstract class DocumentParserTest<Document> {
  static final String BANNER_ID = "b1";
  static final long VERSION = 1;
  static final String CREATIVE_ID = "c1";
  static final String BANNER_GROUP_ID = "bg1";
  static final String LOCALE = "l1";
  static final List<String> THEME_NAME_PLUS_TEMPLATE_ID_STRINGS = Arrays.asList("tt1", "tt2");
  static final String WIDGET_TYPE = "wt1";
  static final String TEAM_TYPE = "MERCHANDISER";
  static final List<String> RELEVANT_STORES = Arrays.asList("rs1", "rs2");
  static final double ASP = 5.39;
  static final double BANNER_GROUP_PRICE_VALUE = 9.36;
  static final List<String> BRANDS = Arrays.asList("bd1", "bd2");
  static final String RELEVANT_STORE_SCORES = "{\"frequencyBasedScore\":{\"s1\":3.0, \"s2\":5.0},\"defaultScore\":{},\"unknown\":{}}";
  static final String SIMILAR_STORE_SCORES = "{}";
  static final String CROSS_STORE_SCORES = "{}";
  static final List<String> EXCLUDED_STORES = Arrays.asList("es1", "es2");

  private DocumentParser<Document> documentParser;

  @Before
  public void before() {
    BannerScorer bannerScorer = new BannerScorer() {
      @Override
      public void updateL1ScoreAndDemandFeatures(IndexedBanner indexedBanner) {}

      @Override
      public Collection<HitBanner> l2Score(Collection<HitBanner> hitBanners, NeoRescoringContext rescoringContext) {
        return null;
      }

    };

    documentParser = getConcreteParser(bannerScorer, new ObjectMapper());
  }

  protected abstract DocumentParser<Document> getConcreteParser(BannerScorer bannerScorer, ObjectMapper objectMapper);

  public void testParsing() throws IOException {
    IndexedBanner banner = documentParser.parse(getDocument());

    Assert.assertEquals(BANNER_ID, banner.getId());
    Assert.assertEquals(CREATIVE_ID, banner.getCreativeId());
    Assert.assertEquals(BANNER_GROUP_ID, banner.getBannerGroupId());
    Assert.assertEquals(LOCALE, banner.getLocale());
    Assert.assertTrue(banner.getThemeNamePlusTemplateIdStrings().containsAll(THEME_NAME_PLUS_TEMPLATE_ID_STRINGS));
    Assert.assertEquals(WIDGET_TYPE, banner.getDemandContext().getContentWidgetType());
    Assert.assertEquals(TEAM_TYPE, banner.getDemandContext().getDemandType());
    Assert.assertTrue(banner.getDemandContext().getContentStores().containsAll(RELEVANT_STORES));
    Assert.assertEquals(ASP, banner.getAsp(), 0);
    Assert.assertEquals(BANNER_GROUP_PRICE_VALUE, banner.getPriceValue(), 0);
    Assert.assertTrue(banner.getBrands().containsAll(BRANDS));

    Assert.assertTrue(banner.getSortedRelevantStoreScores().get("frequencyBasedScore").keySet().containsAll(Arrays.asList("s1","s2")));
    Assert.assertEquals("s2", banner.getSortedRelevantStoreScores().get("frequencyBasedScore").entrySet().iterator().next().getKey());
    Assert.assertTrue(banner.getSortedSimilarStoreScores().isEmpty());
    Assert.assertTrue(banner.getSortedCrossStoreScores().isEmpty());

    Assert.assertTrue(banner.getExcludedStores().containsAll(EXCLUDED_STORES));
  }

  protected abstract Document getDocument() throws JsonProcessingException;
}

package com.flipkart.neo.solr.ltr.schema.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.selector.context.DemandContext;
import com.flipkart.ads.ranking.utils.NeoContextTransformer;
import com.flipkart.neo.selector.context.ProminentBrandQuality;
import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import com.flipkart.neo.solr.ltr.schema.SchemaFieldNames;
import com.flipkart.ad.selector.model.search.BannerSegmentInfo;
import com.flipkart.ad.selector.helper.SegmentTargetingHelper;
import com.flipkart.mm.hazelcast.model.campaign.TeamType;
import org.apache.solr.common.StringUtils;
import com.flipkart.ad.selector.util.BannerFieldsUtils;

public abstract class DocumentParser<Document> {
  private static final TypeReference<Map<String, Map<String, Double>>> mapOfStringToMapOfStringToDoubleTypeRef =
      new TypeReference<Map<String, Map<String, Double>>>() {};
  private static final TypeReference<List<String>> listOfStringsTypeRef = new TypeReference<List<String>>() {};
  private static final TypeReference<Set<String>> setOfStringsTypeRef = new TypeReference<Set<String>>() {};
  private static final TypeReference<Map<String, Double>> mapOfStringToDoubleTypeRef = new TypeReference<Map<String, Double>>() {};
  private static final TypeReference<Map<String, List<String>>> mapOfStringToListTypeRef = new TypeReference<Map<String, List<String>>>() {};

  private final BannerScorer bannerScorer;
  private final ObjectMapper mapper;

  DocumentParser(BannerScorer bannerScorer, ObjectMapper mapper) {
    this.bannerScorer = bannerScorer;
    this.mapper = mapper;
  }

  public IndexedBanner parse(Document document) throws IOException {
    IndexedBanner banner = parserBanner(document);
    bannerScorer.updateL1ScoreAndDemandFeatures(banner);
    return banner;
  }

  public String parseBannerId(Document document) {
    return getStringFieldValue(document, SchemaFieldNames.BANNER_ID);
  }

  public long parseVersion(Document document) {
    Long versionFieldValue = getLongFieldValue(document, SchemaFieldNames.VERSION);
    // if versionField is not found, defaulting to 0.
    return (versionFieldValue == null) ? 0 : versionFieldValue;
  }

  protected abstract Integer getIntegerFieldValue(Document document, String fieldName);
  protected abstract Long getLongFieldValue(Document document, String fieldName);
  protected abstract Double getDoubleFieldValue(Document document, String fieldName);
  protected abstract String getStringFieldValue(Document document, String fieldName);
  protected abstract Collection<String> getStringFieldValues(Document document, String fieldName);

  private IndexedBanner parserBanner(Document document) throws IOException {
    IndexedBanner banner = new IndexedBanner(parseBannerId(document), parseVersion(document));
    String creativeId = getStringFieldValue(document, SchemaFieldNames.CREATIVE_ID);
    String teamType = getStringFieldValue(document, SchemaFieldNames.TEAM_TYPE);

    double asp = getAsp(document);

    banner.setCreativeTemplateId(getStringFieldValue(document, SchemaFieldNames.CREATIVE_TEMPLATE_ID));
    banner.setCreativeId(creativeId);
    banner.setBannerGroupId(getStringFieldValue(document, SchemaFieldNames.BANNER_GROUP_ID));
    banner.setVernacularCreativeGroupId(getStringFieldValue(document, SchemaFieldNames.VERNACULAR_CREATIVE_GROUP_ID));
    banner.setTeamType(TeamType.valueOf(teamType));

    banner.setLocale(getStringFieldValue(document, SchemaFieldNames.LOCALE));

    banner.setThemeNamePlusTemplateIdStrings(getStringFieldValues(document,
        SchemaFieldNames.THEME_NAME_PLUS_TEMPLATE_ID_STRINGS));
    banner.setThemes(getStringFieldValues(document, SchemaFieldNames.THEMES));
    banner.setServingTeamId(Collections.singletonList(getStringFieldValue(document, SchemaFieldNames.SERVING_TEAM_ID)));
    banner.setStoreIds(getStringFieldValues(document, SchemaFieldNames.STORE_IDS));

    banner.setDemandContext(buildDemandContext(creativeId, teamType, asp, document));

    banner.setAsp(asp);
    banner.setPriceValue(getBannerGroupPriceValue(document));

    banner.setBrands(parseStringList(getStringFieldValue(document, SchemaFieldNames.BRANDS)));
    banner.setSortedRelevantStoreScores(parseAndReturnSortedStoreScores(getStringFieldValue(document,
        SchemaFieldNames.RELEVANT_STORE_SCORES)));
    banner.setSortedSimilarStoreScores(parseAndReturnSortedStoreScores(getStringFieldValue(document,
        SchemaFieldNames.SIMILAR_STORE_SCORES)));
    banner.setSortedCrossStoreScores(parseAndReturnSortedStoreScores(getStringFieldValue(document,
        SchemaFieldNames.CROSS_STORE_SCORES)));
    banner.setExcludedStores(parseStringSet(getStringFieldValue(document, SchemaFieldNames.EXCLUDED_STORES)));
    banner.setServingStore(getStringFieldValue(document, SchemaFieldNames.SERVING_STORE));

    banner.setPlacementPriceMap(parseStringMap(getStringFieldValue(document, SchemaFieldNames.PLACEMENT_PRICE_MAP)));
    banner.setBannerSegmentInfo(getBannerSegmentInfo(getStringFieldValue(document, SchemaFieldNames.USER_SEGMENTS)));
    return banner;
  }

  private double getAsp(Document document) {
    Double asp = getDoubleFieldValue(document, SchemaFieldNames.ASP);
    return asp == null ? 0 : asp;
  }

  private double getBannerGroupPriceValue(Document document) {
    Double price = getDoubleFieldValue(document, SchemaFieldNames.BANNER_GROUP_PRICE_VALUE);
    return price == null ? 0 : price;
  }

  private com.flipkart.ads.ranking.model.context.DemandContext buildDemandContext(String creativeId, String teamType,
                                                                                  double asp, Document document) throws IOException {
    Collection<String> affluenceTags = getStringFieldValues(document, SchemaFieldNames.AFFLUENCE_TAGS);
    DemandContext demandContext = DemandContext.newBuilder()
        .setCreativeId(creativeId)
        .setContentStores(parseStringList(getStringFieldValue(document, SchemaFieldNames.RELEVANT_STORES)))
        .setContentWidgetType(getStringFieldValue(document, SchemaFieldNames.WIDGET_TYPE))
        .setStrategyIds(getStrategyIds(document))
        .setCrsId(getStringFieldValue(document, SchemaFieldNames.CRS_ID))
        .setDemandType(teamType)
        .setAsp(asp)
        .setBannerCreator(getStringFieldValue(document, SchemaFieldNames.BANNER_CREATOR))
        .setLocale(getStringFieldValue(document, SchemaFieldNames.LOCALE))
        .setMarketPlace(getStringFieldValue(document, SchemaFieldNames.BANNER_MARKET_PLACE))
        .setCollectionId(getStringFieldValue(document, SchemaFieldNames.COLLECTION_ID))
        .setCreativeTemplate(getStringFieldValue(document, SchemaFieldNames.CREATIVE_TEMPLATE_ID))
        .setBrands(Collections.singletonList(getStringFieldValue(document, SchemaFieldNames.BRAND)))
        .setCrsName(getStringFieldValue(document, SchemaFieldNames.CRS_NAME))
        .setCustomerRating(getStringFieldValue(document, SchemaFieldNames.CUSTOMER_RATING))
        .setMinDiscount(getIntegerFieldValue(document, SchemaFieldNames.MIN_DISCOUNT))
        .setMaxDiscount(getIntegerFieldValue(document, SchemaFieldNames.MAX_DISCOUNT))
        .setMinPrice(getIntegerFieldValue(document, SchemaFieldNames.MIN_PRICE))
        .setMaxPrice(getIntegerFieldValue(document, SchemaFieldNames.MAX_PRICE))
        .setProductIds(Collections.emptyList())
        .setNormalizedASP(getIntegerFieldValue(document, SchemaFieldNames.NORMALIZED_ASP))
        .setAffluenceTags(affluenceTags == null ? null : new ArrayList<>(affluenceTags))
        .setProminentBrandQuality(getProminentBrandQuality(document))
        .build();
    return NeoContextTransformer.demandContext(demandContext);
  }

  protected ProminentBrandQuality getProminentBrandQuality(Document document) {
    return ProminentBrandQuality.newBuilder()
            .setBrandMonetary(getEquivalentBooleanValue(getStringFieldValue(document, SchemaFieldNames.BRAND_MONETARY)))
            .setAvgRating(getStringFieldValue(document, SchemaFieldNames.AVERAGE_RATING))
            .setLowAsp(getBrandTierAsp(document, BannerFieldsUtils.AspBitNumber.LOW_ASP.getValue()))//1 => 1 in binary => first bit set
            .setMidAsp(getBrandTierAsp(document, BannerFieldsUtils.AspBitNumber.MID_ASP.getValue()))//2 => 10 in binary => second bit set
            .setHighAsp(getBrandTierAsp(document, BannerFieldsUtils.AspBitNumber.HIGH_ASP.getValue()))//4 => 100 in binary => third bit set
            .build();
  }

  protected Boolean getEquivalentBooleanValue(String stringFieldValue) {
    if (StringUtils.isEmpty(stringFieldValue))
      return null;
    char val = stringFieldValue.charAt(0);
    return (val == 'T' || val == 't' || val == '1');
  }


  protected Boolean getBrandTierAsp(Document document, int i) {
    if (getIntegerFieldValue(document, SchemaFieldNames.BRAND_VECTOR) == null)
      return null;
    return BannerFieldsUtils.getDecodedBoolean(getIntegerFieldValue(document, SchemaFieldNames.BRAND_VECTOR),i);
  }

  private List<String> getStrategyIds(Document document) {
    Collection<String> values = getStringFieldValues(document, SchemaFieldNames.STRATEGY_IDS);
    if (values == null || values.isEmpty())
      return null;
    return new ArrayList<>(values);
  }

  private List<String> parseStringList(String json) throws IOException {
    if (json == null) return null;
    return mapper.readValue(json, listOfStringsTypeRef);
  }

  private Set<String> parseStringSet(String json) throws IOException {
    if (json == null) return null;
    return mapper.readValue(json, setOfStringsTypeRef);
  }

  private Map<String, LinkedHashMap<String, Double>> parseAndReturnSortedStoreScores(String json) throws IOException {
    if (json == null) return null;

    Map<String, Map<String, Double>> storeScores = mapper.readValue(json, mapOfStringToMapOfStringToDoubleTypeRef);

    Map<String, LinkedHashMap<String, Double>> sortedStoreScores = new HashMap<>(storeScores.size(), 1);
    for (Map.Entry<String, Map<String, Double>> entry: storeScores.entrySet()) {
      sortedStoreScores.put(entry.getKey(), getSortedStoreScores(entry.getValue()));
    }

    return sortedStoreScores;
  }

  private LinkedHashMap<String, Double> getSortedStoreScores(Map<String, Double> storeScores) {
    return storeScores.entrySet().stream()
        .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
            (u,v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); }, LinkedHashMap::new));
  }

  private Map<String, Double> parseStringMap(String json) throws IOException {
    if (json == null) return null;
    return mapper.readValue(json, mapOfStringToDoubleTypeRef);
  }

  private BannerSegmentInfo getBannerSegmentInfo(String json) throws IOException {
    if (json == null) return null;
    Map<String, List<String>> userSegments = mapper.readValue(json, mapOfStringToListTypeRef);
    return SegmentTargetingHelper.getBannerSegmentInfo(userSegments);
  }
}
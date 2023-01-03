package com.flipkart.neo.solr.ltr.client;

import java.io.IOException;
//import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
//import java.util.List;
import java.util.Map;
//import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ad.selector.api.Score;
import com.flipkart.ad.selector.model.allocation.AggregateScorerType;
import com.flipkart.neo.solr.ltr.query.models.RescoringContext;
import com.flipkart.neo.solr.ltr.query.models.context.SupplyInfo;
import com.flipkart.neo.solr.ltr.query.models.context.UserInfo;
import com.flipkart.neo.solr.ltr.query.models.context.VernacularInfo;
import com.flipkart.neo.solr.ltr.query.models.ScoringScheme;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.CommonScoringConfig;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L1ScoringConfig;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L2ScoringConfig;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.ModelScoringConfig;
import com.flipkart.neo.solr.ltr.query.models.ScoringSchemeConfig;
import com.flipkart.neo.solr.ltr.schema.SchemaFieldNames;
import com.flipkart.solr.ltr.client.FKLTRQueryBuildingUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class NeoLTRQueryBuildingUtilTest {

  @Test
  public void testLTRQueryBuilding() throws IOException, SolrServerException {
    // todo:fkltr test query-parsing using RestTestHarness.
    SolrClient solrClient = getSingleNodeSolrClient();

    SolrQuery query = new SolrQuery();
    query.add("q", "*:*");

    FKLTRQueryBuildingUtil<RescoringContext>.Query ltrQuery = getSampleLTRQuery();
    String ltrQueryString = ltrQuery.toString();
    query.add("rq", ltrQueryString);
    query.add("fl", "bannerId,[scoreMeta]");

    query.setRows(NeoLTRQueryBuildingUtil.calculateNumberOfRows(ltrQuery.getRescoringContext().getScoringSchemeConfig()));

    System.out.println(ltrQuery);
    System.out.println(query.jsonStr());
    System.out.println(query.toString());

    QueryResponse queryResponse = solrClient.query(query);
    System.out.println("responseSize: " + queryResponse.getResults().size());
  }

  private FKLTRQueryBuildingUtil<RescoringContext>.Query getSampleLTRQuery() throws JsonProcessingException {
    NeoLTRQueryBuildingUtil queryBuildingUtil = new NeoLTRQueryBuildingUtil(new ObjectMapper());
    RescoringContext rescoringContext = buildNeoRescoringContext();
    return queryBuildingUtil.newQuery(100, rescoringContext);
  }

  @SuppressWarnings("unused")
  private SolrClient getSingleNodeSolrClient() {
    String urlString = "http://localhost:8983/solr/banners";
    return new HttpSolrClient.Builder(urlString).build();
  }

//  @SuppressWarnings("unused")
//  private SolrClient getCloudSolrClient() {
//    List<String> zkHosts = Collections.singletonList("10.32.83.64:2181");
//    String chRoot = "/preprod-solr-a";
//    CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(zkHosts, Optional.of(chRoot))
//        .withConnectionTimeout(1000000).build();
//    cloudSolrClient.setDefaultCollection("banners");
//    return cloudSolrClient;
//  }

  private RescoringContext buildNeoRescoringContext() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    RescoringContext rescoringContext = new RescoringContext();

    rescoringContext.setScoringSchemeConfig(getScoringSchemeConfig());

    rescoringContext.setRequestId(UUID.randomUUID().toString());
    rescoringContext.setRequestTimeStampInMillis(1234567876);
    rescoringContext.setUserId("__USER_ID");

    rescoringContext.setVernacularInfo(getVernacularInfo());

    rescoringContext.setL1ScoringConfig(getL1ScoringConfig());

    rescoringContext.setL2ScoringConfig(getL2ScoringConfig());

    rescoringContext.setUserInfo(getUserInfo());

    rescoringContext.setSupplyInfo(new SupplyInfo());

    System.out.println(mapper.writeValueAsString(rescoringContext));
    return rescoringContext;
  }

  private ScoringSchemeConfig getScoringSchemeConfig() {
    ScoringSchemeConfig scoringSchemeConfig = new ScoringSchemeConfig();
    scoringSchemeConfig.setScoringScheme(ScoringScheme.L1_GROUP_LIMIT);

    scoringSchemeConfig.setGroupField(SchemaFieldNames.THEME_NAME_PLUS_TEMPLATE_ID_STRINGS);

    Map<String, Integer> groupsRequiredWithLimits = new HashMap<>();
    groupsRequiredWithLimits.put("g1", 5);
    groupsRequiredWithLimits.put("g2", 10);
    scoringSchemeConfig.setGroupsRequiredWithLimits(groupsRequiredWithLimits);

    scoringSchemeConfig.setL2Limit(10);

    return scoringSchemeConfig;
  }

  private UserInfo getUserInfo() {
    UserInfo userInfo = new UserInfo();
    userInfo.setAccountId("__USER_ID");

    Map<String, Double> insightsThresholds = new HashMap<>();
    insightsThresholds.put("threshold_for_behavioural_male", 1d);
    insightsThresholds.put("threshold_for_behavioural_female", 1d);
    insightsThresholds.put("threshold_for_demographic_male", 1d);
    insightsThresholds.put("threshold_for_demographic_female", 1d);
    userInfo.setInsightsThresholds(insightsThresholds);

    return userInfo;
  }

  private VernacularInfo getVernacularInfo() {
    VernacularInfo vernacularInfo = new VernacularInfo();
    vernacularInfo.setLocale("en");

    return vernacularInfo;
  }

  private L1ScoringConfig getL1ScoringConfig() {
    L1ScoringConfig l1ScoringConfig = new L1ScoringConfig();
    l1ScoringConfig.setExplorePercentage(30);
    l1ScoringConfig.setTimeBucketInMins(6);
    l1ScoringConfig.setModelId("neo_pctr_l1");
    l1ScoringConfig.setCreativeTemplateIdToL1ScoreMultiplier(new HashMap<>());

    return l1ScoringConfig;
  }

  private L2ScoringConfig getL2ScoringConfig() {
    L2ScoringConfig l2ScoringConfig = new L2ScoringConfig();
    Set<Score.Dimension> scorers = new HashSet<>();
    scorers.add(Score.Dimension.PCTR);
    scorers.add(Score.Dimension.PCVR);
    scorers.add(Score.Dimension.ASP);
    scorers.add(Score.Dimension.PRICE);
    l2ScoringConfig.setScorers(scorers);

    l2ScoringConfig.setAggregateScorerType(AggregateScorerType.CTRCVR);

    CommonScoringConfig aspScoringConfig =  new CommonScoringConfig();
    aspScoringConfig.setFkWeight(0.2);
    aspScoringConfig.setBuWeight(0.2);
    aspScoringConfig.setMaxValue(5000d);
    l2ScoringConfig.setAspScoringConfig(aspScoringConfig);

    CommonScoringConfig priceScoringConfig =  new CommonScoringConfig();
    priceScoringConfig.setFkWeight(0.2);
    priceScoringConfig.setBuWeight(0.2);
    priceScoringConfig.setMaxValue(100d);
    l2ScoringConfig.setPriceScoringConfig(priceScoringConfig);

    ModelScoringConfig pctrScoringConfig =  new ModelScoringConfig();
    pctrScoringConfig.setFkWeight(0.3);
    pctrScoringConfig.setBuWeight(0.3);
    pctrScoringConfig.setMaxValue(0.036);
    pctrScoringConfig.setModelId("neo_pctr");
    pctrScoringConfig.setHyperParameterAlpha(10d);
    pctrScoringConfig.setTimeBucketInMins(2);
    pctrScoringConfig.setExplore(30d);
    l2ScoringConfig.setPctrScoringConfig(pctrScoringConfig);

    ModelScoringConfig pcvrScoringConfig =  new ModelScoringConfig();
    pcvrScoringConfig.setFkWeight(0.5);
    pcvrScoringConfig.setBuWeight(0.5);
    pcvrScoringConfig.setMaxValue(0.0218);
    pcvrScoringConfig.setModelId("neo_pcvr");
    pcvrScoringConfig.setHyperParameterAlpha(100d);
    pcvrScoringConfig.setTimeBucketInMins(2);
    l2ScoringConfig.setPcvrScoringConfig(pcvrScoringConfig);

    return l2ScoringConfig;
  }

}

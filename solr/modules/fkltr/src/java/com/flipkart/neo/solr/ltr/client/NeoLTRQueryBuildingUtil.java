package com.flipkart.neo.solr.ltr.client;

import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.solr.ltr.query.models.RescoringContext;
import com.flipkart.neo.solr.ltr.query.models.ScoringScheme;
import com.flipkart.neo.solr.ltr.query.models.ScoringSchemeConfig;
import com.flipkart.solr.ltr.client.FKLTRQueryBuildingUtil;

/**
 * Neo concrete implementation of FKLTRQueryBuildingUtil.
 * Also provides helper method to calculate ideal value for numOfRows for given rescoringContext.
 */
public class NeoLTRQueryBuildingUtil extends FKLTRQueryBuildingUtil<RescoringContext> {
  private final ObjectMapper objectMapper;

  @SuppressWarnings("WeakerAccess")
  public NeoLTRQueryBuildingUtil(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  protected String getRescoringContextString(RescoringContext rescoringContext) {
    try {
      String rescoringContextString = objectMapper.writeValueAsString(rescoringContext);
      return encloseInDoubleQuotes(rescoringContextString);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * util that encloses a given string in double-quotes and escapes nested ones.
   * eg. {"a":1} becomes "{\"a\":1}"
   * @param input input string
   * @return enclosed string
   */
  private String encloseInDoubleQuotes(String input) throws JsonProcessingException {
    Map<String, String> map = Collections.singletonMap("k", input);
    String mapJsonString = new ObjectMapper().writeValueAsString(map);
    return mapJsonString.substring(5, mapJsonString.length() - 1);
  }

  /**
   * Helper method that provides the ideal number of rows that needs to be set for LTR query to get both correctness
   * and best performance.
   * @return number of rows to be set in query.
   */
  @SuppressWarnings("WeakerAccess")
  public static int calculateNumberOfRows(ScoringSchemeConfig scoringSchemeConfig) {
    if (scoringSchemeConfig.getScoringScheme() == ScoringScheme.L1_GROUP_LIMIT ||
        scoringSchemeConfig.getScoringScheme() == ScoringScheme.L1_GROUP_L2) {
      return scoringSchemeConfig.getGroupsRequiredWithLimits().values().stream().mapToInt(Integer::intValue).sum();
    } else {
      return scoringSchemeConfig.getL2Limit();
    }
  }
}

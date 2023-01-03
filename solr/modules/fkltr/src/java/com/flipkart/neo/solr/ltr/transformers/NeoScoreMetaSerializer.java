package com.flipkart.neo.solr.ltr.transformers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.solr.ltr.transformers.ScoreMetaSerializer;

public class NeoScoreMetaSerializer implements ScoreMetaSerializer<ScoreMeta> {
  // todo:fkltr evaluate serialized size and performance and if required evaluate other techniques.
  private final ObjectMapper objectMapper;

  public NeoScoreMetaSerializer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public String serialize(ScoreMeta scoreMeta) {
    try {
      return objectMapper.writeValueAsString(scoreMeta);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("unable to serialize scoreMeta");
    }
  }
}

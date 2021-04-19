package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.util.Map;

public class ModifyCollectionPayload implements ReflectMapWriter {
  @JsonProperty
  public Integer replicationFactor;

  @JsonProperty
  public Boolean readOnly;

  @JsonProperty
  public String config;

  @JsonProperty
  public Map<String, Object> properties;

  @JsonProperty
  public String async;
}

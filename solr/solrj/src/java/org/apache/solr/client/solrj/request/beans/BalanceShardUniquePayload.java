package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class BalanceShardUniquePayload implements ReflectMapWriter {
  @JsonProperty(required = true)
  public String property;

  @JsonProperty
  public Boolean onlyactivenodes = true;

  @JsonProperty
  public Boolean shardUnique;
}

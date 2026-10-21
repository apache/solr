package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class ParamSetsResponse extends  SolrJerseyResponse {

  @JsonProperty("znodeVersion")
  public int znodeVersion;

  @JsonProperty("params")
  public Map<String, Object> params;

}

package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class ListActiveTaskResponse extends SolrJerseyResponse {
  @JsonProperty
  public Map<String, String> taskList;
}

package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ListActiveTaskResponse extends SolrJerseyResponse {
  @JsonProperty public List<ActiveTaskDetails> taskList;
}

package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;

public class CoreInfoResponse extends SolrJerseyResponse {
  @JsonProperty public String schema;
  @JsonProperty public String host;
  @JsonProperty public Date now;
  @JsonProperty public Date start;
  @JsonProperty public Directory directory;

  public static class Directory {
    @JsonProperty public String cwd;
    @JsonProperty public String instance;
    @JsonProperty public String data;
    @JsonProperty public String dirimpl;
    @JsonProperty public String index;
  }
}

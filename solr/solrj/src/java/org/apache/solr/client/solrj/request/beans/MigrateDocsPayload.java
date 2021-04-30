package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class MigrateDocsPayload implements ReflectMapWriter {
  @JsonProperty(required = true)
  public String target;

  @JsonProperty(required = true)
  public String splitKey;

  @JsonProperty
  public Integer forwardTimeout = 60;

  @JsonProperty
  public Boolean followAliases;

  @JsonProperty
  public String async;
}

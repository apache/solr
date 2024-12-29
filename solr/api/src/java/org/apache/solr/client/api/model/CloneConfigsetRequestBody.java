package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/** Request body for ConfigsetsApi.Clone */
public class CloneConfigsetRequestBody {
  public static final String DEFAULT_CONFIGSET = "_default";

  @JsonProperty(required = true)
  public String name;

  @JsonProperty(defaultValue = DEFAULT_CONFIGSET)
  public String baseConfigSet;

  @JsonProperty public Map<String, Object> properties;
}

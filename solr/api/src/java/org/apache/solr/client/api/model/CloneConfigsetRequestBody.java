package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import org.apache.solr.client.api.endpoint.ConfigsetsApi;

/** Request body for {@link ConfigsetsApi.Clone#cloneExistingConfigSet(CloneConfigsetRequestBody)} */
public class CloneConfigsetRequestBody {
  public static final String DEFAULT_CONFIGSET = "_default";

  @JsonProperty(required = true)
  public String name;

  @JsonProperty(defaultValue = DEFAULT_CONFIGSET)
  public String baseConfigSet;

  @JsonProperty public Map<String, Object> properties;
}

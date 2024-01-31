package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/** Request body for v2 "create core" requests */
public class CreateCoreRequestBody {
  @JsonProperty(required = true)
  public String name;

  @JsonProperty public String instanceDir;

  @JsonProperty public String dataDir;

  @JsonProperty public String ulogDir;

  @JsonProperty public String schema;

  @JsonProperty public String config;

  @JsonProperty public String configSet;

  @JsonProperty public Boolean loadOnStartup;

  // If our JsonProperty clone was more feature-rich here we could specify the property be called
  // 'transient', but without that support it needs to be named something else to avoid conflicting
  // with the 'transient' keyword in Java
  @JsonProperty public Boolean isTransient;

  @JsonProperty public String shard;

  @JsonProperty public String collection;

  // TODO - what type is 'roles' expected to be?
  @JsonProperty public List<String> roles;

  @JsonProperty public String replicaType;

  @JsonProperty public Map<String, Object> properties;

  @JsonProperty public String coreNodeName;

  @JsonProperty public Integer numShards;

  @JsonProperty public Boolean newCollection;

  @JsonProperty public String async;
}

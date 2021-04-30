package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class MoveReplicaPayload implements ReflectMapWriter {
  @JsonProperty(required = true)
  public String targetNode;

  @JsonProperty
  public String replica;

  @JsonProperty
  public String shard;

  @JsonProperty
  public String sourceNode;

  @JsonProperty
  public Boolean waitForFinalState = false;

  @JsonProperty
  public Integer timeout = 600;

  @JsonProperty
  public Boolean inPlaceMove = true;

  @JsonProperty
  public Boolean followAliases;

  // TODO Should this support 'async'? Does 'waitForFinalState' replace 'async' here?
}

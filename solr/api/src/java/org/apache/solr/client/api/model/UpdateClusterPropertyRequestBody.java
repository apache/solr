package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UpdateClusterPropertyRequestBody {
    @JsonProperty(required = true)
    public Object value;
}

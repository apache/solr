package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateCoreResponseBody extends SolrJerseyResponse{
    @JsonProperty
    public String core;
}

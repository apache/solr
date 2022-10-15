package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class RenameClusterPayload implements ReflectMapWriter {
    @JsonProperty
    public String async;

    @JsonProperty
    public Boolean followAliases;

    @JsonProperty(required = true)
    public String to;
}

package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FileStoreRawFileResponse extends SolrJerseyResponse {
  @JsonProperty("content") // A flag value that RawResponseWriter handles specially
  public Object
      output; // Traditionally a 'ContentStream' that the server (via RawResponseWriter) can write
}

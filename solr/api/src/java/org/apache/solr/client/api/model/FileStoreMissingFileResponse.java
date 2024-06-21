package org.apache.solr.client.api.model;

import java.util.Map;

/**
 * One of several potential responses for {@link
 * org.apache.solr.client.api.endpoint.ClusterFileStoreApis#getFile(String, Boolean, String,
 * Boolean)}
 */
public class FileStoreMissingFileResponse extends SolrJerseyResponse {
  public Map<String, Object> files;
}

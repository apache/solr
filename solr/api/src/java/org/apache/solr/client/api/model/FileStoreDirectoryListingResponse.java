package org.apache.solr.client.api.model;

import java.util.Map;

/**
 * One of several possible responses from {@link
 * org.apache.solr.client.api.endpoint.ClusterFileStoreApis#getFile(String, Boolean, String,
 * Boolean)}
 */
public class FileStoreDirectoryListingResponse extends SolrJerseyResponse {
  public Map<String, Object> files;
}

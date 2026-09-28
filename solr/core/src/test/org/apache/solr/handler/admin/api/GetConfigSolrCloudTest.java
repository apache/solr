package org.apache.solr.handler.admin.api;

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

/** HTTP tests for fetching the full config via the collection-scoped v2 path. */
public class GetConfigSolrCloudTest extends SolrCloudTestCase {

  private static final String COLLECTION_NAME = "configApiTestCollection";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty(ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testGetConfigFromCore() throws Exception {
    var request = new ConfigApi.GetConfig(IndexType.COLLECTION, COLLECTION_NAME);
    var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertNull(response.error);
    assertNotNull(response.config);
    assertTrue(response.config.containsKey("luceneMatchVersion"));
    assertTrue(response.config.containsKey("updateHandler"));
    assertTrue(response.config.containsKey("query"));
    assertTrue(response.config.containsKey("requestHandler"));
  }
}

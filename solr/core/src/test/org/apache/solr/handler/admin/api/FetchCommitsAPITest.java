package org.apache.solr.handler.admin.api;

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.ReplicationApi;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * HTTP tests for {@code GET /api/cores/coreName/replication/commits} via the generated SolrJ
 * request.
 */
public class FetchCommitsAPITest extends SolrTestCase {

  private static final String CORE_NAME = "commitsApiTestCore";

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    System.setProperty(ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule.newCollection(CORE_NAME).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Test
  public void testFetchCommits() throws Exception {
    var request = new ReplicationApi.FetchCommits(CORE_NAME);
    var response = request.process(solrTestRule.getSolrClient(CORE_NAME));

    assertNotNull(response);
    assertNull(response.error);
    assertEquals("OK", response.status);
    assertNotNull(response.commits);
    assertFalse(response.commits.isEmpty());
  }
}

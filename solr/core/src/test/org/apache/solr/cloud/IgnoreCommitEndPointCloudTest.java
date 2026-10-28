package org.apache.solr.cloud;

import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.junit.BeforeClass;
import org.junit.Test;

public class IgnoreCommitEndPointCloudTest extends SolrCloudTestCase {

  private static final String COLLECTION = "ignoreCommitEndPoint";
  private static final String CHAIN = "ignore-commit-from-client-403";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-ignore-commit-chain")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
  }

  @Test
  public void testForwardedCommitWithReplicasEndPointPassesIgnoreFactory() throws Exception {
    UpdateRequest req = commitRequest();
    req.setParam(DistributedUpdateProcessor.COMMIT_END_POINT, "replicas");
    UpdateResponse rsp = req.process(cluster.getSolrClient(), COLLECTION);
    assertEquals(0, rsp.getStatus());
  }

  @Test
  public void testClientCommitWithoutEndPointIsStillIgnored() throws Exception {
    UpdateRequest req = commitRequest();
    RemoteSolrException e =
        expectThrows(
            RemoteSolrException.class, () -> req.process(cluster.getSolrClient(), COLLECTION));
    assertEquals(403, e.code());
  }

  private static UpdateRequest commitRequest() {
    UpdateRequest req = new UpdateRequest();
    req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    req.setParam("update.chain", CHAIN);
    return req;
  }
}

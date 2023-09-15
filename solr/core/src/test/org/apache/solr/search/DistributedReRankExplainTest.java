package org.apache.solr.search;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;

import org.apache.solr.cluster.Cluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.RTimerTree;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.*;

import org.apache.solr.SolrTestCaseJ4;


@SolrTestCaseJ4.SuppressSSL
public class DistributedReRankExplainTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int numShards = 2;
  private static final String COLLECTIONORALIAS = "collection1";

  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    String collection = COLLECTIONORALIAS;
    configureCluster(2)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .configure();
    CollectionAdminRequest.createCollection(collection, "conf1", 2, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collection, 2, 2);
  }

  @Test
  public void testReRankExplain() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    UpdateRequest updateRequest = new UpdateRequest();
    for(int i=0; i<100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(CommonParams.ID, Integer.toString(i));
      doc.addField("test_s", "hello");
      updateRequest.add(doc);
    }
    updateRequest.process(client, COLLECTIONORALIAS);
    client.commit(COLLECTIONORALIAS);

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    String reRank = "{!rerank reRankDocs=10 reRankMainScale=0-10 reRankQuery='test_s:hello'}";
    solrParams.add("q", "test_s:hello").add("debugQuery", "true").add(CommonParams.RQ, reRank);
    QueryRequest queryRequest = new QueryRequest(solrParams);
    QueryResponse queryResponse = queryRequest.process(client, COLLECTIONORALIAS);
    Map<String, Object> debug = queryResponse.getDebugMap();
    assertNotNull(debug);
    String explain = debug.get("explain").toString();
    assertTrue(explain.contains("5.0101576 = combined scaled first and unscaled second pass score "));
    
    solrParams = new ModifiableSolrParams();
    reRank = "{!rerank reRankDocs=10 reRankScale=0-10 reRankQuery='test_s:hello'}";
    solrParams.add("q", "test_s:hello").add("debug", "true").add(CommonParams.RQ, reRank);
    queryRequest = new QueryRequest(solrParams);
    queryResponse = queryRequest.process(client, COLLECTIONORALIAS);
    debug = queryResponse.getDebugMap();
    assertNotNull(debug);
    explain = debug.get("explain").toString();
    assertTrue(explain.contains("10.005078 = combined unscaled first and scaled second pass score "));
  }

}
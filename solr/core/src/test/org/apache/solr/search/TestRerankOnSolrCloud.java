package org.apache.solr.search;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for ReRankQParserPlugin functionality
 */
public class TestRerankOnSolrCloud extends SolrCloudTestCase {

    private static final String COLLECTION_NAME = "rerank_test";

    @BeforeClass
    public static void setupCluster() throws Exception {
        System.setProperty("managed.schema.mutable", "true");
        configureCluster(2)
                .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
                .configure();

        // Create collection
        CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1", 2, 1)
                .process(cluster.getSolrClient());

        cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);

        // Prepare test data
        CloudSolrClient client = cluster.getSolrClient();
        UpdateRequest updateRequest = new UpdateRequest();

        // Add test documents
        for (int i = 1; i <= 5; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(CommonParams.ID, String.valueOf(i));
            doc.addField("test_s", "hello");
            doc.addField("test_i", i);
            updateRequest.add(doc);
        }

        updateRequest.process(client, COLLECTION_NAME);
        client.commit(COLLECTION_NAME);
    }

    @Test
    public void testLargeReRankDocsParamWithoutSort() throws Exception {
        // Build query parameters
        SolrParams params = params(
                CommonParams.Q, "*:*",
                CommonParams.FL, "id,test_s,test_i,score",
                CommonParams.RQ, "{!rerank reRankQuery='{!func} 100' reRankDocs=1000000000 reRankWeight=2}"
        );

        QueryRequest req = new QueryRequest(params);
        QueryResponse rsp = req.process(cluster.getSolrClient(), COLLECTION_NAME);

        // Test passes if execution completes without OOM
        Assert.assertTrue(true);
    }

    @Test
    public void testLargeReRankDocsParamWithSort() throws Exception {
        // Build query parameters
        SolrParams params = params(
                CommonParams.Q, "*:*",
                CommonParams.FL, "id,test_s,test_i,score",
                CommonParams.RQ, "{!rerank reRankQuery='{!func} 100' reRankDocs=1000000000 reRankWeight=2}",
                CommonParams.SORT, "test_i asc"
        );

        QueryRequest req = new QueryRequest(params);
        QueryResponse rsp = req.process(cluster.getSolrClient(), COLLECTION_NAME);

        // Test passes if execution completes without OOM
        Assert.assertTrue(true);
    }
}

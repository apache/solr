package org.apache.solr.handler.component;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DistributedCombinedQueryComponentTest extends BaseDistributedSearchTestCase {

    private static final int NUM_DOCS = 10;
    private static final String vectorField = "vector";

    @BeforeClass
    public static void setUpClass() throws Exception {
        initCore("solrconfig-combined-query.xml", "schema-vector-catchall.xml");
        System.setProperty("validateAfterInactivity", "200");
        System.setProperty("solr.httpclient.retries", "0");
        System.setProperty("distribUpdateSoTimeout", "5000");
    }

    private void prepareIndexDocs() throws Exception {
        List<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 1; i <= NUM_DOCS; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", Integer.toString(i));
            doc.addField("text", "test text for doc " + i);
            doc.addField("title", "title test for doc " + i);
            docs.add(doc);
        }
        // cosine distance vector1= 1.0
        docs.get(0).addField(vectorField, Arrays.asList(1f, 2f, 3f, 4f));
        // cosine distance vector1= 0.998
        docs.get(1).addField(vectorField, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f));
        // cosine distance vector1= 0.992
        docs.get(2).addField(vectorField, Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f));
        // cosine distance vector1= 0.999
        docs.get(3).addField(vectorField, Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f));
        // cosine distance vector1= 0.862
        docs.get(4).addField(vectorField, Arrays.asList(30f, 22f, 35f, 20f));
        // cosine distance vector1= 0.756
        docs.get(5).addField(vectorField, Arrays.asList(40f, 1f, 1f, 200f));
        // cosine distance vector1= 0.970
        docs.get(6).addField(vectorField, Arrays.asList(5f, 10f, 20f, 40f));
        // cosine distance vector1= 0.515
        docs.get(7).addField(vectorField, Arrays.asList(120f, 60f, 30f, 15f));
        // cosine distance vector1= 0.554
        docs.get(8).addField(vectorField, Arrays.asList(200f, 50f, 100f, 25f));
        // cosine distance vector1= 0.997
        docs.get(9).addField(vectorField, Arrays.asList(1.8f, 2.5f, 3.7f, 4.9f));
        del("*:*");
        for (SolrInputDocument doc : docs) {indexDoc(doc);}
        commit();
    }

    public void testSingleLexicalQuery() throws Exception {
        prepareIndexDocs();
        QueryResponse rsp;
        rsp = queryServer(createParams(CommonParams.JSON, "{\"queries\":" +
                "{\"lexical1\":{\"lucene\":{\"query\":\"id:2^=10\"}}}," +
                "\"limit\":5," +
                "\"fields\":[\"id\",\"score\",\"title\"]," +
                "\"params\":{\"combiner\":true,\"combiner.upTo\":10,\"combiner.query\":[\"lexical1\"]}}",
                "shards", getShardsString()));
        assertEquals(1, rsp.getResults().size());
        assertFieldValues(rsp.getResults(), id, "2");
    }

    public void testMultipleLexicalQuery() throws Exception {
        prepareIndexDocs();
        QueryResponse rsp;
        rsp = queryServer(createParams(CommonParams.JSON, "{\"queries\":" +
                        "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}}," +
                        "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}}," +
                        "\"limit\":5," +
                        "\"fields\":[\"id\",\"score\",\"title\"]," +
                        "\"params\":{\"combiner\":true,\"combiner.upTo\":100,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
                        "shards", getShardsString()));
        assertEquals(5, rsp.getResults().size());
        assertFieldValues(rsp.getResults(), id, "1", "2", "3", "4", "5");
    }

    public void testHybridQuery() throws Exception {
        prepareIndexDocs();
        // lexical => 2,3
        // vector => 1,4,2,10,3
        QueryResponse rsp;
        rsp = queryServer(createParams(CommonParams.JSON, "{\"queries\":" +
                        "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}}," +
                        "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}}," +
                        "\"limit\":4," +
                        "\"fields\":[\"id\",\"score\",\"title\"]," +
                        "\"params\":{\"combiner\":true,\"combiner.upTo\":10,\"combiner.query\":[\"lexical\",\"vector\"]}}",
                        "shards", getShardsString()));
        assertEquals(4, rsp.getResults().size());
        assertFieldValues(rsp.getResults(), id, "2", "3", "1", "4");
    }

}
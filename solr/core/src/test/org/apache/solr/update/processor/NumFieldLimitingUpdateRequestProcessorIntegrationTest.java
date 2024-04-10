package org.apache.solr.update.processor;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.checkerframework.checker.signature.qual.DotSeparatedIdentifiersOrPrimitiveType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class NumFieldLimitingUpdateRequestProcessorIntegrationTest extends SolrCloudTestCase {

    private static long ADMIN_OPERATION_TIMEOUT = 15 * 1000;
    private static String SINGLE_SHARD_COLL_NAME = "singleShardColl";
    private static String FIELD_LIMITING_CS_NAME = "fieldLimitingConfig";

    @BeforeClass
    public static void setupCluster() throws Exception {
        final var configPath = TEST_PATH().resolve("configsets").resolve("cloud-minimal-field-limiting").resolve("conf");
        configureCluster(1)
                .addConfig(FIELD_LIMITING_CS_NAME, configPath)
                .configure();

        final var createRequest = CollectionAdminRequest.createCollection(SINGLE_SHARD_COLL_NAME, FIELD_LIMITING_CS_NAME, 1, 1);
        createRequest.process(cluster.getSolrClient());
        cluster.waitForActiveCollection(SINGLE_SHARD_COLL_NAME, 20, TimeUnit.SECONDS, 1, 1);
    }

    private void setFieldLimitTo(int value) throws Exception {
        final var setCollPropRequest = CollectionAdminRequest.setCollectionProperty(SINGLE_SHARD_COLL_NAME, "solr.test.maxFields", String.valueOf(value));
        final var collPropResponse = setCollPropRequest.process(cluster.getSolrClient());
        assertEquals(0, collPropResponse.getStatus());

        final var reloadRequest = CollectionAdminRequest.reloadCollection(SINGLE_SHARD_COLL_NAME);
        final var reloadResponse = reloadRequest.process(cluster.getSolrClient());
        assertEquals(0, reloadResponse.getStatus());
    }

    @Test
    public void test() throws Exception {
        setFieldLimitTo(100);

        // Add 100 new fields - now error should be thrown since only the final commit in loop pushes us over the limit
        for (int i = 0 ; i < 5; i++) {
            final var docList = getDocumentListToAddFields(20);
            final var updateResponse = cluster.getSolrClient(SINGLE_SHARD_COLL_NAME).add(docList);
            assertEquals(0, updateResponse.getStatus());
            cluster.getSolrClient(SINGLE_SHARD_COLL_NAME).commit();
        }

        // Adding any additional docs should hit the limit, since the updates+commit above caused us to exceed.
        final Exception thrown = expectThrows(Exception.class, () -> {
            final var docList = getDocumentListToAddFields(10);
            final var updateResponse = cluster.getSolrClient(SINGLE_SHARD_COLL_NAME).add(docList);
            cluster.getSolrClient(SINGLE_SHARD_COLL_NAME).commit();
        });
        assertNotNull(thrown);

        // Set the limit higher and reload
        setFieldLimitTo(300);
        // Check that we can once again index.
    }

    private Collection<SolrInputDocument> getDocumentListToAddFields(int numFieldsToAdd) {
        int fieldsAdded = 0;
        final var docList = new ArrayList<SolrInputDocument>();
        while (fieldsAdded < numFieldsToAdd) {
            final var doc = new SolrInputDocument();
            doc.addField("id", randomFieldValue());

            final int fieldsForDoc = Math.min(numFieldsToAdd - fieldsAdded, 5);
            for (int fieldCount = 0 ; fieldCount < fieldsForDoc; fieldCount++) {
                doc.addField(randomFieldName(), randomFieldValue());
            }
            fieldsAdded += fieldsForDoc;
            docList.add(doc);
        }

        return docList;
    }

    private String randomFieldName() {
        return UUID.randomUUID().toString().replace("-", "_") + "_s";
    }

    private String randomFieldValue() {
        return UUID.randomUUID().toString();
    }
}

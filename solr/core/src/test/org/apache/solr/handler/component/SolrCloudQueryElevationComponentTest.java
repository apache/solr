package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

public class SolrCloudQueryElevationComponentTest extends SolrCloudTestCase {

    private static final String COLLECTION = "collection";
    private static final String CONFIG_WITH_ELEVATION_FILE = "cloud-elevation";
    private static final String CONFIG_WITHOUT_ELEVATION_FILE = "cloud-elevation-2";
    private static final String ELEVATE_FILE = "elevate.xml";

    private CloudSolrClient client;

    @BeforeClass
    public static void setupCluster() throws Exception {
        configureCluster(1)
                .addConfig(CONFIG_WITH_ELEVATION_FILE, configset(CONFIG_WITH_ELEVATION_FILE))
                .addConfig(CONFIG_WITHOUT_ELEVATION_FILE, configset(CONFIG_WITHOUT_ELEVATION_FILE))
                .configure();
    }

    @Before
    public void setupSolrClient() {
        client = cluster.getSolrClient();
        client.setDefaultCollection(COLLECTION);
    }

    @After
    public void deleteCollection() throws Exception {
        CollectionAdminRequest.deleteCollection(COLLECTION).process(client);
    }

    @Test
    public void testElevationFileInConfigDir() throws Exception {
        setupCollectionWithConfig(CONFIG_WITH_ELEVATION_FILE);
        checkQueryWithElevation();
    }

    @Test
    public void testElevationFileInDataDir() throws IOException, SolrServerException {
        // Copy elevate.xml to the data dir as a user would do manually.
        // Create an advance the data dir before the collection is actually created
        // and copy the elevate.xml there.
        FileUtils.createDirectories(getCoreDataDir());
        Path sourceElevateFile = configset(CONFIG_WITH_ELEVATION_FILE).resolve(ELEVATE_FILE);
        Path targetElevateFile = getCoreDataDir().resolve(ELEVATE_FILE);
        FileUtils.copyFile(sourceElevateFile.toFile(), targetElevateFile.toFile());

        setupCollectionWithConfig(CONFIG_WITHOUT_ELEVATION_FILE);
        checkQueryWithElevation();
    }

    private Path getCoreDataDir() {
        //TODO: is there a better way to get the data dir of the only core of the collection?
        return cluster.getBaseDir().resolve("node1").resolve(COLLECTION + "_shard1_replica_n1").resolve("data");
    }

    private void setupCollectionWithConfig(String configName) throws IOException, SolrServerException {
        CollectionAdminRequest.createCollection(COLLECTION, configName, 1, 1).process(client);
        cluster.waitForActiveCollection(COLLECTION, 1, 1);
    }

    private void checkQueryWithElevation() throws IOException, SolrServerException {
        client.add(sdoc("id", "1", "text", "bill"));
        client.add(sdoc("id", "2", "text", "will"));
        client.commit();

        SolrQuery query = new SolrQuery("foo bar bill")
                .setFields("id,[elevated]")
                .setParam("enableElevation", "true")
                .setParam("forceElevation", "true");
        QueryResponse response = client.query(query);

        assertEquals(2, response.getResults().getNumFound());
        SolrDocument document = response.getResults().get(0);
        assertEquals(true, document.getFieldValue("[elevated]"));
    }
}

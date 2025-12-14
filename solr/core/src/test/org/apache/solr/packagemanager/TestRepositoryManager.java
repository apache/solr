package org.apache.solr.packagemanager;

import java.nio.file.Files;

import org.apache.solr.cli.CLITestHelper;
import org.apache.solr.cli.ToolRuntime;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.filestore.ClusterFileStore;
import org.junit.Assert;
import org.junit.Before;

public class TestRepositoryManager extends SolrCloudTestCase {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    configureCluster(1).addConfig("conf", configset("conf3")).configure();
    
//    CollectionAdminRequest.createCollection("collection1", "conf", 1, 1)
//        .process(cluster.getSolrClient());
//    cluster.waitForActiveCollection("collection1", 1, 1);
  }
  
  public void testAddKey() throws Exception {
    String testFileName = "TestRepositoryManager.txt";
    String expectedPath = ClusterFileStore.KEYS_DIR + "/" + testFileName;
    RepositoryManager mngr = new RepositoryManager(cluster.getSolrClient(), null);
   
    mngr.addKey("TestRepositoryManager".getBytes(), testFileName);
    Assert.assertTrue("File should exist", Files.exists(testSolrHome.resolve(expectedPath)));
  }

}

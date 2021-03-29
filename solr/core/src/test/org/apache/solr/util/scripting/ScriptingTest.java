package org.apache.solr.util.scripting;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class ScriptingTest extends SolrCloudTestCase {

  private static final String COLLECTION = ScriptingTest.class.getSimpleName() + "_collection";

  private static SolrCloudManager cloudManager;
  private static CoreContainer cc;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    cc = cluster.getJettySolrRunner(0).getCoreContainer();
    cloudManager = cc.getZkController().getSolrCloudManager();
  }

  @After
  public void cleanup() throws Exception {
    cluster.deleteAllCollections();
  }

  String testBasicScript =
      "# simple comment\n" +
          "// another comment\n" +
          "      \n" +
          "solr_request /admin/collections?action=CREATE&name=" + COLLECTION + "&numShards=2&nrtReplicas=2\n" +
          "wait_collection collection=" + COLLECTION + "&shards=2&replicas=2\n" +
          "ctx_set key=myNode&value=${_random_node_}\n" +
          "solr_request /admin/collections?action=ADDREPLICA&collection=" + COLLECTION + "&shard=shard1&node=${myNode}\n" +
          "ctx_set key=myNode&value=${_random_node_}\n" +
          "solr_request /admin/collections?action=ADDREPLICA&collection=" + COLLECTION + "&shard=shard1&node=${myNode}\n" +
          "loop_start iterations=${iterative}\n" +
          "  solr_request /admin/collections?action=ADDREPLICA&collection=" + COLLECTION + "&shard=shard1&node=${myNode}\n" +
          "  solr_request /admin/collections?action=ADDREPLICA&collection=" + COLLECTION + "&shard=shard1&node=${myNode}\n" +
          "loop_end\n" +
          "dump\n";

  @Test
  public void testBasicScript() throws Exception {
    Script script = Script.load(cluster.getSolrClient(), null, testBasicScript);
    // use string value, otherwise PropertiesUtil won't work properly
    script.context.put("iterative", "2");
    script.run();
  }

  @Test
  public void testLoadResource() throws Exception {

  }

  public void testErrorHandling() throws Exception {

  }
}

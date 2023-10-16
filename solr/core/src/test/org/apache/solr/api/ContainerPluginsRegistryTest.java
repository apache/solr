package org.apache.solr.api;

import java.nio.file.Path;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementConfig;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link ContainerPluginsRegistry} */
public class ContainerPluginsRegistryTest extends SolrTestCaseJ4 {

  private CoreContainer cc;
  private Path solrHome;

  @Before
  public void beforeTest() {
    solrHome = createTempDir();
  }

  @After
  public void afterTest() {
    if (null != cc) {
      cc.shutdown();
    }
    cc = null;
  }

  @Test
  public void testPluginIsLoadedFromSolrXml() {
    String solrXml =
        "<solr><containerPlugin "
            + "name=\"custom-request-handler\" "
            + "class=\"org.apache.solr.api.ContainerPluginsRegistryTest$CustomRequestHandler\""
            + "/></solr>";
    cc = createCoreContainer(solrHome, solrXml);

    ContainerPluginsRegistry.ApiInfo apiInfo = getPlugin("custom-request-handler");
    assertNull(apiInfo.getInfo().version);
    assertNull(apiInfo.getInfo().pathPrefix);
    assertTrue(apiInfo.getInstance() instanceof CustomRequestHandler);
  }

  @Test
  public void testPluginIsLoadedFromSolrXmlWithVersion() {
    String solrXml =
        "<solr><containerPlugin "
            + "name=\"custom-request-handler\" "
            + "class=\"org.apache.solr.api.ContainerPluginsRegistryTest$CustomRequestHandler\" "
            + "version=\"1.0.0\""
            + "/></solr>";
    cc = createCoreContainer(solrHome, solrXml);

    ContainerPluginsRegistry.ApiInfo apiInfo = getPlugin("custom-request-handler");
    assertEquals("1.0.0", apiInfo.getInfo().version);
  }

  @Test
  public void testPluginIsLoadedFromSolrXmlWithPathPrefix() {
    String solrXml =
        "<solr><containerPlugin "
            + "name=\"custom-request-handler\" "
            + "class=\"org.apache.solr.api.ContainerPluginsRegistryTest$CustomRequestHandler\" "
            + "path-prefix=\"foo\""
            + "/></solr>";
    cc = createCoreContainer(solrHome, solrXml);

    ContainerPluginsRegistry.ApiInfo apiInfo = getPlugin("custom-request-handler");
    assertEquals("foo", apiInfo.getInfo().pathPrefix);
  }

  @Test
  public void testPluginIsLoadedFromSolrXmlWithConfig() {
    String solrXml =
        "<solr><containerPlugin "
            + "name=\".placement-plugin\" "
            + "class=\"org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory\">"
            + "<int name=\"minimalFreeDiskGB\">20</int>"
            + "<int name=\"prioritizedFreeDiskGB\">200</int>"
            + "<lst name=\"withCollection\">"
            + "  <str name=\"A_primary\">A_secondary</str>"
            + "  <str name=\"B_primary\">B_secondary</str>"
            + "</lst>"
            + "</containerPlugin></solr>";
    cc = createCoreContainer(solrHome, solrXml);

    ContainerPluginsRegistry.ApiInfo apiInfo = getPlugin(".placement-plugin");
    AffinityPlacementConfig config = (AffinityPlacementConfig) apiInfo.getInfo().config;
    assertEquals(20, config.minimalFreeDiskGB);
    assertEquals(200, config.prioritizedFreeDiskGB);
    assertEquals(
        Map.of("A_primary", "A_secondary", "B_primary", "B_secondary"), config.withCollection);
  }

  public static class CustomRequestHandler {}

  private ContainerPluginsRegistry.ApiInfo getPlugin(String name) {
    ContainerPluginsRegistry.ApiInfo apiInfo = cc.getContainerPluginsRegistry().getPlugin(name);
    assertNotNull("Plugin not registered", apiInfo);
    assertEquals(name, apiInfo.getInfo().name);
    return apiInfo;
  }
}

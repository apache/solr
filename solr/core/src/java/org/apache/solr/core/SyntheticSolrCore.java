package org.apache.solr.core;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.rest.RestManager;

/**
 * A synthetic core that is created only in memory and not registered against Zookeeper.
 *
 * <p>This is only used in Coordinator node to support a subset of SolrCore functionalities required
 * by Coordinator operations such as aggregating and writing out response and providing configset
 * info.
 *
 * <p>There should only be one instance of SyntheticSolrCore per configset
 */
public class SyntheticSolrCore extends SolrCore {
  public SyntheticSolrCore(CoreContainer coreContainer, CoreDescriptor cd, ConfigSet configSet) {
    super(coreContainer, cd, configSet);
  }

  public static SyntheticSolrCore createAndRegisterCore(
      CoreContainer coreContainer, String syntheticCoreName, String configSetName) {
    Map<String, String> coreProps = new HashMap<>();
    coreProps.put(CoreAdminParams.CORE_NODE_NAME, coreContainer.getHostName());
    coreProps.put(CoreAdminParams.COLLECTION, syntheticCoreName);

    CoreDescriptor syntheticCoreDescriptor =
        new CoreDescriptor(
            syntheticCoreName,
            Paths.get(coreContainer.getSolrHome() + "/" + syntheticCoreName),
            coreProps,
            coreContainer.getContainerProperties(),
            coreContainer.getZkController());

    ConfigSet coreConfig =
        coreContainer.getConfigSetService().loadConfigSet(syntheticCoreDescriptor, configSetName);
    syntheticCoreDescriptor.setConfigSetTrusted(coreConfig.isTrusted());
    SyntheticSolrCore syntheticCore =
        new SyntheticSolrCore(coreContainer, syntheticCoreDescriptor, coreConfig);
    coreContainer.registerCore(syntheticCoreDescriptor, syntheticCore, false, false);

    return syntheticCore;
  }

  @Override
  protected void bufferUpdatesIfConstructing(CoreDescriptor coreDescriptor) {
    // no updates to SyntheticSolrCore
  }

  @Override
  protected RestManager initRestManager() throws SolrException {
    // returns an initialized RestManager. As init routines requires reading configname of the
    // core's collection from ZK
    // which synthetic core is not registered in ZK.
    // We do not expect RestManager ops on Coordinator Nodes
    return new RestManager();
  }

  @Override
  public void close() {
    super.close();
  }
}

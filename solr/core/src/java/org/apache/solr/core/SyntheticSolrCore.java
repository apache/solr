/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import java.nio.file.Path;
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
    Map<String, String> coreProps = Map.of(CoreAdminParams.COLLECTION, syntheticCoreName);

    CoreDescriptor syntheticCoreDescriptor =
        new CoreDescriptor(
            syntheticCoreName,
            Path.of(coreContainer.getSolrHome(), syntheticCoreName),
            coreProps,
            coreContainer.getContainerProperties(),
            coreContainer.getZkController());
    syntheticCoreDescriptor.setConfigSet(configSetName);
    ConfigSet coreConfig =
        coreContainer.getConfigSetService().loadConfigSet(syntheticCoreDescriptor);
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
}

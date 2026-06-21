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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SolrCloud ConfigSetService impl.
 */
public class CloudConfigSetService extends ConfigSetService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final ZkController zkController;

  public CloudConfigSetService(SolrResourceLoader loader, boolean shareSchema, ZkController zkController) {
    super(loader, shareSchema);
    this.zkController = zkController;
  }

  @Override
  public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
    final String colName = cd.getCollectionName();

    // Resolve the configSet name. The authoritative source is the collection's configName in ZK; prefer it
    // over the COLL_CONF core property, which is captured at core-creation time and is NOT refreshed when the
    // collection's config is changed via MODIFYCOLLECTION — a reloaded core must pick up the new config.
    String configSetName = null;
    try {
      configSetName = zkController.getZkStateReader().readConfigName(colName);
    } catch (KeeperException ex) {
      log.warn("Could not read configName from ZK for collection {}; falling back to core property", colName, ex);
    }
    if (configSetName == null) {
      configSetName = cd.getCoreProperty(CollectionAdminParams.COLL_CONF, null);
    }
    if (configSetName == null) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Trouble resolving configSet for collection " + colName);
    }
    // Always record the resolved configset name on the descriptor. Otherwise cd.getConfigSet() stays null in
    // cloud and ConfigSetService.createIndexSchema can't build its cache key — defeating shareSchema (cores of
    // the same configset would each get a distinct IndexSchema). See CoreDescriptor.getConfigSet()'s TODO.
    cd.setConfigSet(configSetName);

    return new ZkSolrResourceLoader(cd.getInstanceDir(), configSetName, null, zkController);
  }

  @Override
  protected NamedList loadConfigSetFlags(CoreDescriptor cd, SolrResourceLoader loader) {
    try {
      return ConfigSetProperties.readFromResourceLoader(loader, ".");
    } catch (Exception ex) {
      if (log.isDebugEnabled()) log.debug("No configSet flags", ex);
      return null;
    }
  }

  @Override
  protected Long getCurrentSchemaModificationVersion(String configSet, SolrConfig solrConfig, String schemaFile) {
    String zkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet + "/" + schemaFile;
    Stat stat;
    try {
      stat = zkController.getZkClient().exists(zkPath, null, true);
    } catch (KeeperException e) {
      log.warn("Unexpected exception when getting modification time of {}", zkPath, e);
      return null; // debatable; we'll see an error soon if there's a real problem
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    if (stat == null) { // not found
      return null;
    }
    return (long) stat.getVersion();
  }

  @Override
  public String configSetName(CoreDescriptor cd) {
    return "configset " + cd.getConfigSet();
  }
}

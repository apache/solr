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

package org.apache.solr.zero.client;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to create a new ZeroStoreClient with underlying {@link BackupRepository}. BackupRepository
 * properties are stored in solr.xml in the {@code <zero> ... </zero>} section
 */
public class ZeroStoreClientFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static ZeroStoreClient newInstance(NodeConfig cfg, SolrMetricManager metricManager) {

    return new ZeroStoreClient(
        newZeroRepository(cfg),
        metricManager,
        cfg.getZeroConfig().getNumFilePusherThreads(),
        cfg.getZeroConfig().getNumFilePullerThreads());
  }

  public static BackupRepository newZeroRepository(NodeConfig cfg) {

    SolrResourceLoader ressourceLoader = cfg.getSolrResourceLoader();
    ZeroConfig zeroConfig = cfg.getZeroConfig();
    PluginInfo[] repoPlugins = zeroConfig.getBackupRepositoryPlugins();
    BackupRepositoryFactory zeroRepositoryFactory = new BackupRepositoryFactory(repoPlugins);

    BackupRepository repository;

    List<String> enabledRepos =
        Arrays.stream(repoPlugins)
            .filter(PluginInfo::isEnabled)
            .map(p -> p.name)
            .collect(Collectors.toList());
    if (enabledRepos.isEmpty()) {
      List<String> allRepos =
          Arrays.stream(repoPlugins).map(p -> p.name).collect(Collectors.toList());
      if (allRepos.isEmpty()) {
        log.warn("No Zero store BackupRepository defined, using default");
        repository = zeroRepositoryFactory.newInstance(ressourceLoader);
      } else if (allRepos.size() == 1) {
        repository = zeroRepositoryFactory.newInstance(ressourceLoader, allRepos.get(0));
      } else {
        log.warn(
            "Multiple not enabled Zero store BackupRepository defined ({}), using first: {}",
            StrUtils.join(allRepos, ','),
            allRepos.get(0));
        repository = zeroRepositoryFactory.newInstance(ressourceLoader, allRepos.get(0));
      }
    } else if (enabledRepos.size() == 1) {
      repository = zeroRepositoryFactory.newInstance(ressourceLoader, enabledRepos.get(0));
    } else {
      log.warn(
          "Multiple enabled Zero store BackupRepository defined ({}), using first: {}",
          StrUtils.join(enabledRepos, ','),
          enabledRepos.get(0));
      repository = zeroRepositoryFactory.newInstance(ressourceLoader, enabledRepos.get(0));
    }

    return repository;
  }
}

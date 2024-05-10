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

package org.apache.solr.core.backup.repository;

import static org.apache.solr.core.backup.repository.DelegatingBackupRepository.PARAM_DELEGATE_REPOSITORY_NAME;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupRepositoryFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, PluginInfo> backupRepoPluginByName = new HashMap<>();
  private PluginInfo defaultBackupRepoPlugin = null;

  public BackupRepositoryFactory(PluginInfo[] backupRepoPlugins) {
    if (backupRepoPlugins != null) {
      for (int i = 0; i < backupRepoPlugins.length; i++) {
        String name = backupRepoPlugins[i].name;
        boolean isDefault = backupRepoPlugins[i].isDefault();

        if (backupRepoPluginByName.containsKey(name)) {
          throw new SolrException(
              ErrorCode.SERVER_ERROR, "Duplicate backup repository with name " + name);
        }
        if (isDefault) {
          if (this.defaultBackupRepoPlugin != null) {
            throw new SolrException(
                ErrorCode.SERVER_ERROR, "More than one backup repository is configured as default");
          }
          this.defaultBackupRepoPlugin = backupRepoPlugins[i];
        }
        backupRepoPluginByName.put(name, backupRepoPlugins[i]);
        log.info("Added backup repository with configuration params {}", backupRepoPlugins[i]);
      }
      if (backupRepoPlugins.length == 1) {
        this.defaultBackupRepoPlugin = backupRepoPlugins[0];
      }

      if (this.defaultBackupRepoPlugin != null) {
        log.info(
            "Default configuration for backup repository is with configuration params {}",
            defaultBackupRepoPlugin);
      }
    }
  }

  public BackupRepository newInstance(SolrResourceLoader loader, String name) {
    Objects.requireNonNull(loader);
    Objects.requireNonNull(name);
    PluginInfo repo = getBackupRepoPlugin(name);
    BackupRepository backupRepository = loader.newInstance(repo.className, BackupRepository.class);
    backupRepository.init(repo.initArgs);

    if (backupRepository instanceof DelegatingBackupRepository) {
      DelegatingBackupRepository delegatingRepo = (DelegatingBackupRepository) backupRepository;
      String delegateName = (String) repo.initArgs.get(PARAM_DELEGATE_REPOSITORY_NAME);
      if (delegateName == null) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "Missing '"
                + PARAM_DELEGATE_REPOSITORY_NAME
                + "' parameter for backup repository with name "
                + name);
      }
      PluginInfo delegatePlugin = getBackupRepoPlugin(delegateName);
      BackupRepository delegate =
          loader.newInstance(delegatePlugin.className, BackupRepository.class);
      delegate.init(delegatingRepo.getDelegateInitArgs(delegatePlugin.initArgs));
      delegatingRepo.setDelegate(delegate);
    }

    return backupRepository;
  }

  private PluginInfo getBackupRepoPlugin(String name) {
    return Objects.requireNonNull(
        backupRepoPluginByName.get(name), "Could not find a backup repository with name " + name);
  }

  public BackupRepository newInstance(SolrResourceLoader loader) {
    if (defaultBackupRepoPlugin != null) {
      return newInstance(loader, defaultBackupRepoPlugin.name);
    }
    LocalFileSystemRepository repo = new LocalFileSystemRepository();
    repo.init(new NamedList<>());
    return repo;
  }
}

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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;
import static org.apache.solr.util.FileUtils.normalizeToOsPathSeparator;

import jakarta.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.client.api.endpoint.CoreApis;
import org.apache.solr.client.api.model.CoreStatusResponse;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.RefCounted;

/**
 * V2 APIs for getting the status of one or all cores.
 *
 * <p>This API (GET /v2/cores/coreName is analogous to the v1 /admin/cores?action=status command.
 */
public class CoreStatus extends CoreAdminAPIBase implements CoreApis.GetStatus {

  private static boolean INDEX_INFO_DEFAULT_VALUE = true;

  @Inject
  public CoreStatus(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public CoreStatusResponse getAllCoreStatus(Boolean indexInfo) throws IOException {
    final var indexInfoNeeded = indexInfo == null ? INDEX_INFO_DEFAULT_VALUE : indexInfo;
    return fetchStatusInfo(coreContainer, null, indexInfoNeeded);
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public CoreStatusResponse getCoreStatus(String coreName, Boolean indexInfo) throws IOException {
    final var indexInfoNeeded = indexInfo == null ? INDEX_INFO_DEFAULT_VALUE : indexInfo;
    return fetchStatusInfo(coreContainer, coreName, indexInfoNeeded);
  }

  public static CoreStatusResponse fetchStatusInfo(
      CoreContainer coreContainer, String coreName, Boolean indexInfo) throws IOException {
    final var response = new CoreStatusResponse();
    response.initFailures = new HashMap<>();

    for (Map.Entry<String, CoreContainer.CoreLoadFailure> failure :
        coreContainer.getCoreInitFailures().entrySet()) {

      // Skip irrelevant initFailures if we're only interested in a single core
      if (coreName != null && !failure.getKey().equals(coreName)) continue;

      response.initFailures.put(failure.getKey(), failure.getValue().exception);
    }

    // Populate status for each core
    final var coreNameList =
        (coreName != null) ? List.of(coreName) : coreContainer.getAllCoreNames();
    if (coreNameList.size() > 1) coreNameList.sort(null);
    response.status = new HashMap<>();
    for (String toPopulate : coreNameList) {
      response.status.put(toPopulate, getCoreStatus(coreContainer, toPopulate, indexInfo));
    }

    return response;
  }

  /**
   * Returns the core status for a particular core.
   *
   * @param cores - the enclosing core container
   * @param cname - the core to return
   * @param isIndexInfoNeeded - add what may be expensive index information. NOT returned if the
   *     core is not loaded
   * @return - a named list of key/value pairs from the core.
   * @throws IOException - LukeRequestHandler can throw an I/O exception
   */
  public static CoreStatusResponse.SingleCoreData getCoreStatus(
      CoreContainer cores, String cname, boolean isIndexInfoNeeded) throws IOException {
    final var info = new CoreStatusResponse.SingleCoreData();
    if (cores.isCoreLoading(cname)) {
      info.name = cname;
      info.isLoaded = false;
      info.isLoading = true;
    } else {
      if (!cores.isLoaded(cname)) { // Lazily-loaded core, fill in what we can.
        // It would be a real mistake to load the cores just to get the status
        CoreDescriptor desc = cores.getCoreDescriptor(cname);
        if (desc != null) {
          info.name = desc.getName();
          info.instanceDir = desc.getInstanceDir().toString();
          // None of the following are guaranteed to be present in a not-yet-loaded core.
          String tmp = desc.getDataDir();
          if (StrUtils.isNotBlank(tmp)) info.dataDir = tmp;
          tmp = desc.getConfigName();
          if (StrUtils.isNotBlank(tmp)) info.config = tmp;
          tmp = desc.getSchemaName();
          if (StrUtils.isNotBlank(tmp)) info.schema = tmp;
          info.isLoaded = false;
        }
      } else {
        try (SolrCore core = cores.getCore(cname)) {
          if (core != null) {
            info.name = core.getName();
            info.instanceDir = core.getInstancePath().toString();
            info.dataDir = normalizeToOsPathSeparator(core.getDataDir());
            info.config = core.getConfigResource();
            info.schema = core.getSchemaResource();
            info.startTime = core.getStartTimeStamp();
            info.uptime = core.getUptimeMs();
            if (cores.isZooKeeperAware()) {
              info.lastPublished =
                  core.getCoreDescriptor()
                      .getCloudDescriptor()
                      .getLastPublished()
                      .toString()
                      .toLowerCase(Locale.ROOT);
              info.configVersion = core.getSolrConfig().getZnodeVersion();
              final var cloudInfo = new CoreStatusResponse.CloudDetails();
              cloudInfo.collection =
                  core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
              cloudInfo.shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();
              cloudInfo.replica = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
              cloudInfo.replicaType =
                  core.getCoreDescriptor().getCloudDescriptor().getReplicaType().name();
              info.cloud = cloudInfo;
            }
            if (isIndexInfoNeeded) {
              RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
              try {
                final var indexInfo =
                    LukeRequestHandler.getIndexInfo(searcher.get().getIndexReader());
                long size = core.getIndexSize();
                indexInfo.sizeInBytes = size;
                indexInfo.size = NumberUtils.readableSize(size);
                info.index = indexInfo;
              } finally {
                searcher.decref();
              }
            }
          }
        }
      }
    }
    return info;
  }
}

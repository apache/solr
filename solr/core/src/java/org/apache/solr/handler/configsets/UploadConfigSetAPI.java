/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.configsets;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.PUT;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API for uploading a new configset (or overwriting an existing one).
 *
 * <p>This API (PUT /v2/cluster/configs/configsetName) is analogous to the v1
 * /admin/configs?action=UPLOAD command.
 */
public class UploadConfigSetAPI extends ConfigSetAPIBase {

  public static final String CONFIGSET_NAME_PLACEHOLDER = "name";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public UploadConfigSetAPI(CoreContainer coreContainer) {
    super(coreContainer);
  }

  @EndPoint(method = PUT, path = "/cluster/configs/{name}", permission = CONFIG_EDIT_PERM)
  public void uploadConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ensureConfigSetUploadEnabled();

    final String configSetName = req.getPathTemplateValues().get("name");
    boolean overwritesExisting = configSetService.checkConfigExists(configSetName);
    boolean requestIsTrusted =
        isTrusted(req.getUserPrincipal(), coreContainer.getAuthenticationPlugin());
    // Get upload parameters
    boolean allowOverwrite = req.getParams().getBool(ConfigSetParams.OVERWRITE, true);
    boolean cleanup = req.getParams().getBool(ConfigSetParams.CLEANUP, false);
    final InputStream inputStream = ensureNonEmptyInputStream(req);

    if (overwritesExisting && !allowOverwrite) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The configuration " + configSetName + " already exists");
    }

    List<String> filesToDelete;
    if (overwritesExisting && cleanup) {
      filesToDelete = configSetService.getAllConfigFiles(configSetName);
    } else {
      filesToDelete = Collections.emptyList();
    }

    // Create a node for the configuration in zookeeper
    // For creating the baseZnode, the cleanup parameter is only allowed to be true when
    // singleFilePath is not passed.
    createBaseNode(configSetService, overwritesExisting, requestIsTrusted, configSetName);

    try (ZipInputStream zis = new ZipInputStream(inputStream, StandardCharsets.UTF_8)) {
      boolean hasEntry = false;
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        hasEntry = true;
        String filePath = zipEntry.getName();
        filesToDelete.remove(filePath);
        if (!zipEntry.isDirectory()) {
          configSetService.uploadFileToConfig(configSetName, filePath, zis.readAllBytes(), true);
        }
      }
      if (!hasEntry) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Either empty zipped data, or non-zipped data was uploaded. In order to upload a configSet, you must zip a non-empty directory to upload.");
      }
    }
    deleteUnusedFiles(configSetService, configSetName, filesToDelete);

    // If the request is doing a full trusted overwrite of an untrusted configSet (overwrite=true,
    // cleanup=true), then trust the configSet.
    if (cleanup
        && requestIsTrusted
        && overwritesExisting
        && !configSetService.isConfigSetTrusted(configSetName)) {
      Map<String, Object> metadata = Collections.singletonMap("trusted", true);
      configSetService.setConfigMetadata(configSetName, metadata);
    }
  }

  private void deleteUnusedFiles(
      ConfigSetService configSetService, String configName, List<String> filesToDelete)
      throws IOException {
    if (!filesToDelete.isEmpty()) {
      if (log.isInfoEnabled()) {
        log.info("Cleaning up {} unused files", filesToDelete.size());
      }
      if (log.isDebugEnabled()) {
        log.debug("Cleaning up unused files: {}", filesToDelete);
      }
      configSetService.deleteFilesFromConfig(configName, filesToDelete);
    }
  }
}

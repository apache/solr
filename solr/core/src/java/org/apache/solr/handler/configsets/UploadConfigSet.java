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
package org.apache.solr.handler.configsets;

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.solr.client.api.endpoint.ConfigsetsApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.FileTypeMagicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadConfigSet extends ConfigSetAPIBase implements ConfigsetsApi.Upload {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Inject
  public UploadConfigSet(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public SolrJerseyResponse uploadConfigSet(
      String configSetName, Boolean overwrite, Boolean cleanup, InputStream requestBody)
      throws IOException {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureConfigSetUploadEnabled();

    boolean overwritesExisting = configSetService.checkConfigExists(configSetName);
    boolean requestIsTrusted =
        isTrusted(solrQueryRequest.getUserPrincipal(), coreContainer.getAuthenticationPlugin());
    // Get upload parameters
    if (overwrite == null) overwrite = true;
    if (cleanup == null) cleanup = false;

    if (overwritesExisting && !overwrite) {
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

    try (ZipInputStream zis = new ZipInputStream(requestBody, StandardCharsets.UTF_8)) {
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
      configSetService.setConfigSetTrust(configSetName, true);
    }
    return response;
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public SolrJerseyResponse uploadConfigSetFile(
      String configSetName,
      String filePath,
      Boolean overwrite,
      Boolean cleanup,
      InputStream requestBody)
      throws IOException {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureConfigSetUploadEnabled();

    boolean overwritesExisting = configSetService.checkConfigExists(configSetName);
    boolean requestIsTrusted =
        isTrusted(solrQueryRequest.getUserPrincipal(), coreContainer.getAuthenticationPlugin());

    // Get upload parameters

    String singleFilePath = filePath != null ? filePath : "";
    if (overwrite == null) overwrite = true;
    if (cleanup == null) cleanup = false;

    String fixedSingleFilePath = singleFilePath;
    if (fixedSingleFilePath.charAt(0) == '/') {
      fixedSingleFilePath = fixedSingleFilePath.substring(1);
    }
    byte[] data = requestBody.readAllBytes();
    if (fixedSingleFilePath.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The file path provided for upload, '" + singleFilePath + "', is not valid.");
    } else if (ZkMaintenanceUtils.isFileForbiddenInConfigSets(fixedSingleFilePath)
        || FileTypeMagicUtil.isFileForbiddenInConfigset(data)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The file type provided for upload, '"
              + singleFilePath
              + "', is forbidden for use in configSets.");
    } else if (cleanup) {
      // Cleanup is not allowed while using singleFilePath upload
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "ConfigSet uploads do not allow cleanup=true when file path is used.");
    } else {
      // Create a node for the configuration in config
      // For creating the baseNode, the cleanup parameter is only allowed to be true when
      // singleFilePath is not passed.
      createBaseNode(configSetService, overwritesExisting, requestIsTrusted, configSetName);
      configSetService.uploadFileToConfig(configSetName, fixedSingleFilePath, data, overwrite);
    }
    return response;
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

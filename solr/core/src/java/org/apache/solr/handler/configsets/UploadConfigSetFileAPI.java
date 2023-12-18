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

import java.io.InputStream;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for adding or updating a single file within a configset.
 *
 * <p>This API (PUT /v2/cluster/configs/configsetName/someFilePath) is analogous to the v1
 * /admin/configs?action=UPLOAD&amp;filePath=someFilePath command.
 */
public class UploadConfigSetFileAPI extends ConfigSetAPIBase {

  public static final String CONFIGSET_NAME_PLACEHOLDER =
      UploadConfigSetAPI.CONFIGSET_NAME_PLACEHOLDER;
  public static final String FILEPATH_PLACEHOLDER = "*";

  private static final String API_PATH =
      "/cluster/configs/{" + CONFIGSET_NAME_PLACEHOLDER + "}/" + FILEPATH_PLACEHOLDER;

  public UploadConfigSetFileAPI(CoreContainer coreContainer) {
    super(coreContainer);
  }

  @EndPoint(method = PUT, path = API_PATH, permission = CONFIG_EDIT_PERM)
  public void updateConfigSetFile(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ensureConfigSetUploadEnabled();

    final String configSetName = req.getPathTemplateValues().get("name");
    boolean overwritesExisting = configSetService.checkConfigExists(configSetName);
    boolean requestIsTrusted =
        isTrusted(req.getUserPrincipal(), coreContainer.getAuthenticationPlugin());

    // Get upload parameters

    String singleFilePath = req.getPathTemplateValues().getOrDefault(FILEPATH_PLACEHOLDER, "");
    boolean allowOverwrite = req.getParams().getBool(ConfigSetParams.OVERWRITE, true);
    boolean cleanup = req.getParams().getBool(ConfigSetParams.CLEANUP, false);
    final InputStream inputStream = ensureNonEmptyInputStream(req);

    String fixedSingleFilePath = singleFilePath;
    if (fixedSingleFilePath.charAt(0) == '/') {
      fixedSingleFilePath = fixedSingleFilePath.substring(1);
    }
    if (fixedSingleFilePath.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The file path provided for upload, '" + singleFilePath + "', is not valid.");
    } else if (ZkMaintenanceUtils.isFileForbiddenInConfigSets(fixedSingleFilePath)) {
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
      configSetService.uploadFileToConfig(
          configSetName, fixedSingleFilePath, inputStream.readAllBytes(), allowOverwrite);
    }
  }
}

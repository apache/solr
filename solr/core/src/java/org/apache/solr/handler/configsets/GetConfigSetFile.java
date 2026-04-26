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

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import jakarta.inject.Inject;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import org.apache.solr.client.api.endpoint.ConfigsetsApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API implementation for reading the contents of a single file from an existing configset. */
public class GetConfigSetFile extends ConfigSetAPIBase implements ConfigsetsApi.GetFile {

  @Inject
  public GetConfigSetFile(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public StreamingOutput getConfigSetFile(String configSetName, String filePath) throws Exception {
    if (StrUtils.isNullOrEmpty(configSetName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No configset name provided");
    }
    if (StrUtils.isNullOrEmpty(filePath)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No file path provided");
    }
    if (!configSetService.checkConfigExists(configSetName)) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND, "ConfigSet '" + configSetName + "' not found");
    }

    // Return StreamingOutput that writes raw bytes (supports both text and binary files)
    return output -> {
      try {
        final byte[] data = configSetService.downloadFileFromConfig(configSetName, filePath);
        if (data == null) {
          throw new SolrException(
              SolrException.ErrorCode.NOT_FOUND,
              "File '" + filePath + "' not found in configset '" + configSetName + "'");
        }
        output.write(data);
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.NOT_FOUND,
            "File '" + filePath + "' not found in configset '" + configSetName + "'",
            e);
      }
    };
  }
}

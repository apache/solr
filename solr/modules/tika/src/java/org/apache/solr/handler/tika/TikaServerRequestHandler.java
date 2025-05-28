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
package org.apache.solr.handler.tika;

import java.io.Closeable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.eclipse.jetty.client.HttpClient;

public class TikaServerRequestHandler extends ContentStreamHandlerBase
    implements SolrCoreAware, PermissionNameProvider, Closeable {

  private String tikaServerUrl;
  private int connectionTimeout;
  private int socketTimeout;
  private String idField;
  private boolean returnMetadata;
  private String metadataPrefix;
  private String contentField;
  private HttpClient jettyHttpClient;

  @Override
  public void inform(SolrCore core) {
    SolrParams params = initArgs;
    if (params == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "TikaServerRequestHandler initArgs are missing.");
    }

    tikaServerUrl = params.get("tikaServer.url");
    if (tikaServerUrl == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Missing required parameter: tikaServer.url");
    }

    connectionTimeout = params.getInt("tikaServer.connectionTimeout", 5000);
    socketTimeout = params.getInt("tikaServer.socketTimeout", 60000);
    idField = params.get("tikaServer.idField"); // Can be null if not specified
    returnMetadata = params.getBool("tikaServer.returnMetadata", true);
    metadataPrefix = params.get("tikaServer.metadataPrefix", "");
    contentField = params.get("tikaServer.contentField", "content");

    this.jettyHttpClient = new HttpClient();
    this.jettyHttpClient.setConnectTimeout(this.connectionTimeout);
    this.jettyHttpClient.setIdleTimeout(this.socketTimeout);
    // Potentially set an executor if Solr has a shared one, e.g.
    // this.jettyHttpClient.setExecutor(core.getCoreContainer().getUpdateShardExecutor());
    // For now, default executor is fine.
    try {
      this.jettyHttpClient.start();
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Failed to start Jetty HttpClient", e);
    }
  }

  @Override
  protected ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    return new TikaServerDocumentLoader(
        req,
        processor,
        this.tikaServerUrl,
        this.connectionTimeout,
        this.socketTimeout,
        this.idField,
        this.returnMetadata,
        this.metadataPrefix,
        this.contentField,
        this.jettyHttpClient);
  }

  @Override
  public String getDescription() {
    return "Extracts content and metadata from rich documents using an external Tika Server.";
  }

  @Override
  public Name getPermissionName(org.apache.solr.security.AuthorizationContext request) {
    // TODO: Define appropriate permission if needed, for now, allow based on existing Solr request
    // handler permissions
    return Name.ALL; // Or a more specific permission
  }

  @Override
  public void close() throws java.io.IOException {
    if (this.jettyHttpClient != null) {
      try {
        this.jettyHttpClient.stop();
      } catch (Exception e) {
        throw new java.io.IOException("Failed to stop Jetty HttpClient", e);
      }
    }
  }
}

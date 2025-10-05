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
package org.apache.solr.handler.extraction;

import java.io.IOException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Handler for rich documents like PDF or Word or any other file format that Tika handles that need
 * the text to be extracted first from the document.
 */
public class ExtractingRequestHandler extends ContentStreamHandlerBase
    implements SolrCoreAware, PermissionNameProvider {

  public static final String PARSE_CONTEXT_CONFIG = "parseContext.config";
  public static final String CONFIG_LOCATION = "tika.config";
  public static final String TIKASERVER_URL = "tikaserver.url";

  protected String tikaConfigLoc;
  protected ParseContextConfig parseContextConfig;

  protected SolrContentHandlerFactory factory;
  protected ExtractionBackendFactory backendFactory;
  protected String defaultBackendName;

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  @Override
  public void inform(SolrCore core) {
    try {
      // Store tika config location (backend-specific)
      this.tikaConfigLoc = (String) initArgs.get(CONFIG_LOCATION);

      String parseContextConfigLoc = (String) initArgs.get(PARSE_CONTEXT_CONFIG);
      if (parseContextConfigLoc == null) { // default:
        parseContextConfig = new ParseContextConfig();
      } else {
        parseContextConfig =
            new ParseContextConfig(core.getResourceLoader(), parseContextConfigLoc);
      }

      // Initialize backend factory once; backends are created lazily on demand
      String tikaServerUrl = (String) initArgs.get(TIKASERVER_URL);
      backendFactory =
          new ExtractionBackendFactory(core, tikaConfigLoc, parseContextConfig, tikaServerUrl);

      // Choose default backend name (do not instantiate yet)
      String backendName = (String) initArgs.get(ExtractingParams.EXTRACTION_BACKEND);
      defaultBackendName =
          (backendName == null || backendName.trim().isEmpty())
              ? LocalTikaExtractionBackend.NAME
              : backendName;

    } catch (Exception e) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR, "Unable to initialize ExtractingRequestHandler", e);
    }

    factory = new SolrContentHandlerFactory();
  }

  @Override
  protected ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    // Allow per-request override of backend via request param
    String backendParam = req.getParams().get(ExtractingParams.EXTRACTION_BACKEND);
    String nameToUse =
        (backendParam != null && !backendParam.trim().isEmpty())
            ? backendParam
            : defaultBackendName;
    ExtractionBackend extractionBackend = backendFactory.getBackend(nameToUse);
    return new ExtractingDocumentLoader(req, processor, factory, extractionBackend);
  }

  // ////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getDescription() {
    return "Add/Update Rich document";
  }

  @Override
  public void close() throws IOException {
    super.close();
    backendFactory.close();
  }
}

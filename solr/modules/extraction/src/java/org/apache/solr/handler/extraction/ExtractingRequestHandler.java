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

  protected String tikaConfigLoc;
  protected ParseContextConfig parseContextConfig;

  protected SolrContentHandlerFactory factory;
  protected ExtractionBackend backend;

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
    } catch (Exception e) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR, "Unable to initialize ExtractingRequestHandler", e);
    }

    factory = createFactory();

    // Choose backend implementation
    String backendName = (String) initArgs.get("extraction.backend");
    try {
      if (backendName == null
          || backendName.trim().isEmpty()
          || backendName.equalsIgnoreCase("local")) {
        backend = new LocalTikaExtractionBackend(core, tikaConfigLoc, parseContextConfig);
      } else if (backendName.equalsIgnoreCase("dummy")) {
        backend = new DummyExtractionBackend();
      } else {
        // Fallback to local if unknown
        backend = new LocalTikaExtractionBackend(core, tikaConfigLoc, parseContextConfig);
      }
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to initialize extraction backend", e);
    }
  }

  protected SolrContentHandlerFactory createFactory() {
    return new SolrContentHandlerFactory();
  }

  @Override
  protected ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    // Allow per-request override of backend via request param "extraction.backend"
    ExtractionBackend backendToUse = this.backend;
    String backendParam = req.getParams().get("extraction.backend");
    if (backendParam != null) {
      if (backendParam.equalsIgnoreCase("dummy")) {
        backendToUse = new DummyExtractionBackend();
      } else if (backendParam.equalsIgnoreCase("local")) {
        try {
          backendToUse =
              new LocalTikaExtractionBackend(req.getCore(), tikaConfigLoc, parseContextConfig);
        } catch (Exception e) {
          throw new SolrException(
              ErrorCode.SERVER_ERROR, "Unable to initialize extraction backend", e);
        }
      }
      // unknown values fall back to the handler-configured backend
    }
    return new ExtractingDocumentLoader(req, processor, factory, backendToUse);
  }

  // ////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getDescription() {
    return "Add/Update Rich document";
  }
}

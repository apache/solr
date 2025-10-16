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

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for rich documents like PDF or Word or any other file format that Tika handles that need
 * the text to be extracted first from the document.
 */
@SuppressWarnings("removal")
public class ExtractingRequestHandler extends ContentStreamHandlerBase
    implements SolrCoreAware, PermissionNameProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PARSE_CONTEXT_CONFIG = "parseContext.config";
  public static final String CONFIG_LOCATION = "tika.config";

  protected String tikaConfigLoc;
  protected ParseContextConfig parseContextConfig;

  protected SolrContentHandlerFactory factory;
  protected String defaultBackendName;
  protected LocalTikaExtractionBackend localBackend;
  protected TikaServerExtractionBackend tikaServerBackend; // may be null if not configured

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

      // Always create local backend
      this.localBackend = new LocalTikaExtractionBackend(core, tikaConfigLoc, parseContextConfig);

      // Optionally create Tika Server backend if URL configured
      String tikaServerUrl = (String) initArgs.get(ExtractingParams.TIKASERVER_URL);
      if (tikaServerUrl != null && !tikaServerUrl.trim().isEmpty()) {
        int timeoutSecs = 0;
        Object initTimeout = initArgs.get(ExtractingParams.TIKASERVER_TIMEOUT_SECS);
        if (initTimeout != null) {
          try {
            timeoutSecs = Integer.parseInt(String.valueOf(initTimeout));
          } catch (NumberFormatException nfe) {
            throw new SolrException(
                ErrorCode.SERVER_ERROR,
                "Invalid value for '"
                    + ExtractingParams.TIKASERVER_TIMEOUT_SECS
                    + "': "
                    + initTimeout,
                nfe);
          }
        }
        Object maxCharsObj = initArgs.get(ExtractingParams.TIKASERVER_MAX_CHARS);
        long maxCharsLimit = TikaServerExtractionBackend.DEFAULT_MAXCHARS_LIMIT;
        if (maxCharsObj != null) {
          try {
            maxCharsLimit = Long.parseLong(String.valueOf(maxCharsObj));
          } catch (NumberFormatException nfe) {
            throw new SolrException(
                ErrorCode.SERVER_ERROR,
                "Invalid value for '"
                    + ExtractingParams.TIKASERVER_MAX_CHARS
                    + "': "
                    + maxCharsObj);
          }
        }
        this.tikaServerBackend =
            new TikaServerExtractionBackend(tikaServerUrl, timeoutSecs, initArgs, maxCharsLimit);
      }

      // Choose default backend name
      String backendName = (String) initArgs.get(ExtractingParams.EXTRACTION_BACKEND);
      this.defaultBackendName =
          (backendName == null || backendName.trim().isEmpty())
              ? LocalTikaExtractionBackend.NAME
              : backendName;

      // Validate backend and check configuration
      switch (this.defaultBackendName) {
        case LocalTikaExtractionBackend.NAME:
          break;
        case TikaServerExtractionBackend.NAME:
          // Tika Server backend requires URL to be configured
          if (this.tikaServerBackend == null) {
            throw new SolrException(
                ErrorCode.INVALID_STATE, "Tika Server backend requested but no URL configured");
          }
          break;
        default:
          throw new SolrException(
              ErrorCode.BAD_REQUEST,
              "Invalid extraction backend: '"
                  + this.defaultBackendName
                  + "'. Must be one of: '"
                  + LocalTikaExtractionBackend.NAME
                  + "', '"
                  + TikaServerExtractionBackend.NAME
                  + "'");
      }
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

    ExtractionBackend extractionBackend;
    if (LocalTikaExtractionBackend.NAME.equals(nameToUse)) {
      extractionBackend = localBackend;
    } else if (TikaServerExtractionBackend.NAME.equals(nameToUse)) {
      if (tikaServerBackend == null) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Tika Server backend requested but '"
                + ExtractingParams.TIKASERVER_URL
                + "' is not configured");
      }
      extractionBackend = tikaServerBackend;
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown extraction backend: " + nameToUse);
    }

    return new ExtractingDocumentLoader(req, processor, factory, extractionBackend);
  }

  // ////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getDescription() {
    return "Add/Update Rich document";
  }

  @Override
  public void close() throws IOException {
    // Close our backends to release any shared resources (e.g., Jetty HttpClient)
    try {
      if (tikaServerBackend != null) {
        tikaServerBackend.close();
      }
    } finally {
      try {
        if (localBackend != null) {
          localBackend.close();
        }
      } finally {
        super.close();
      }
    }
  }
}

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
public class ExtractingRequestHandler extends ContentStreamHandlerBase
    implements SolrCoreAware, PermissionNameProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected SolrContentHandlerFactory factory;
  protected String defaultBackendName;
  protected TikaServerExtractionBackend tikaServerBackend;

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  @Override
  public void inform(SolrCore core) {
    try {
      // Fail if using old unsupported configuration
      if (initArgs.get("tika.config") != null || initArgs.get("parseContext.config") != null) {
        if (log.isErrorEnabled()) {
          log.error(
              "The 'tika.config' and 'parseContext.config' parameters are no longer supported since Solr 10.");
        }
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "The 'tika.config' and 'parseContext.config' parameters are no longer supported since Solr 10.");
      }

      // Handle backend selection
      String backendName = (String) initArgs.get(ExtractingParams.EXTRACTION_BACKEND);
      this.defaultBackendName =
          (backendName == null || backendName.trim().isEmpty())
              ? TikaServerExtractionBackend.NAME
              : backendName;

      // Validate backend name
      if (!TikaServerExtractionBackend.NAME.equals(this.defaultBackendName)) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "Invalid extraction backend: '"
                + this.defaultBackendName
                + "'. Only '"
                + TikaServerExtractionBackend.NAME
                + "' is supported");
      }

      String tikaServerUrl = (String) initArgs.get(ExtractingParams.TIKASERVER_URL);
      if (tikaServerUrl == null || tikaServerUrl.trim().isEmpty()) {
        if (log.isErrorEnabled()) {
          log.error(
              "Tika Server URL must be configured via '{}' parameter",
              ExtractingParams.TIKASERVER_URL);
        }
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "Tika Server URL must be configured via '"
                + ExtractingParams.TIKASERVER_URL
                + "' parameter");
      }

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
              "Invalid value for '" + ExtractingParams.TIKASERVER_MAX_CHARS + "': " + maxCharsObj);
        }
      }
      this.tikaServerBackend =
          new TikaServerExtractionBackend(tikaServerUrl, timeoutSecs, initArgs, maxCharsLimit);
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
    if (TikaServerExtractionBackend.NAME.equals(nameToUse)) {
      extractionBackend = tikaServerBackend;
    } else {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Unknown extraction backend: '"
              + nameToUse
              + "'. Only '"
              + TikaServerExtractionBackend.NAME
              + "' is supported");
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
    // Close the backend to release any shared resources (e.g., Jetty HttpClient)
    try {
      if (tikaServerBackend != null) {
        tikaServerBackend.close();
      }
    } finally {
      super.close();
    }
  }
}

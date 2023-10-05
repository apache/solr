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

import static org.apache.solr.handler.admin.ConfigSetsHandler.CONFIG_SET_TIMEOUT;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthenticationPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parent class for all APIs that manipulate configsets
 *
 * <p>Contains utilities for tasks common in configset manipulation, including running configset
 * "commands" and checking configset "trusted-ness".
 */
public class ConfigSetAPIBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final CoreContainer coreContainer;
  protected final DistributedCollectionConfigSetCommandRunner distributedConfigSetCommandRunner;

  protected final ConfigSetService configSetService;

  public ConfigSetAPIBase(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    this.distributedConfigSetCommandRunner = coreContainer.getDistributedConfigSetCommandRunner();
    this.configSetService = coreContainer.getConfigSetService();
  }

  protected void runConfigSetCommand(
      SolrQueryResponse rsp,
      ConfigSetParams.ConfigSetAction action,
      Map<String, Object> messageToSend)
      throws Exception {
    if (log.isInfoEnabled()) {
      log.info("Invoked ConfigSet Action :{} with params {} ", action.toLower(), messageToSend);
    }

    distributedConfigSetCommandRunner.runConfigSetCommand(
        rsp, action, messageToSend, CONFIG_SET_TIMEOUT);
  }

  protected void ensureConfigSetUploadEnabled() {
    if (!"true".equals(System.getProperty("configset.upload.enabled", "true"))) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Configset upload feature is disabled. To enable this, start Solr with '-Dconfigset.upload.enabled=true'.");
    }
  }

  protected InputStream ensureNonEmptyInputStream(SolrQueryRequest req) throws IOException {
    Iterator<ContentStream> contentStreamsIterator = req.getContentStreams().iterator();

    if (!contentStreamsIterator.hasNext()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "No stream found for the config data to be uploaded");
    }

    return contentStreamsIterator.next().getStream();
  }

  public static boolean isTrusted(Principal userPrincipal, AuthenticationPlugin authPlugin) {
    if (authPlugin != null && userPrincipal != null) {
      log.debug("Trusted configset request");
      return true;
    }
    log.debug("Untrusted configset request");
    return false;
  }

  protected void createBaseNode(
      ConfigSetService configSetService,
      boolean overwritesExisting,
      boolean requestIsTrusted,
      String configName)
      throws IOException {
    Map<String, Object> metadata = Collections.singletonMap("trusted", requestIsTrusted);

    if (overwritesExisting) {
      if (!requestIsTrusted) {
        ensureOverwritingUntrustedConfigSet(configName);
      }
      // If the request is trusted and cleanup=true, then the configSet will be set to trusted after
      // the overwriting has been done.
    } else {
      configSetService.setConfigMetadata(configName, metadata);
    }
  }

  /*
   * Fail if an untrusted request tries to update a trusted ConfigSet
   */
  private void ensureOverwritingUntrustedConfigSet(String configName) throws IOException {
    boolean isCurrentlyTrusted = configSetService.isConfigSetTrusted(configName);
    if (isCurrentlyTrusted) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Trying to make an untrusted ConfigSet update on a trusted configSet");
    }
  }
}

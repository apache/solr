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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Security Configuration Handler which works on standalone local files */
public class SecurityConfHandlerLocal extends SecurityConfHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected Path securityJsonPath;
  private final Object securityJsonLock = new Object();

  public SecurityConfHandlerLocal(CoreContainer coreContainer) {
    super(coreContainer);
    securityJsonPath = coreContainer.getSolrHome().resolve("security.json");
  }

  /**
   * Serializes requests, since a local file has no versioned write to detect concurrent edits the
   * way ZooKeeper does. Reads are included so they never see a partially written file.
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    synchronized (securityJsonLock) {
      super.handleRequestBody(req, rsp);
    }
  }

  /**
   * Fetches security props from SOLR_HOME
   *
   * @param getFresh NOP
   * @return SecurityConfig whose data property either contains security.json, or an empty map if
   *     not found
   */
  @Override
  public SecurityConfig getSecurityConfig(boolean getFresh) {
    if (Files.exists(securityJsonPath)) {
      try (InputStream securityJsonIs = Files.newInputStream(securityJsonPath)) {
        return new SecurityConfig().setData(securityJsonIs);
      } catch (Exception e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Failed opening existing security.json file: " + securityJsonPath,
            e);
      }
    }
    return new SecurityConfig();
  }

  @Override
  protected void getConf(SolrQueryResponse rsp, String key) {
    SecurityConfig props = getSecurityConfig(false);
    Object o = props.getData().get(key);
    if (o == null) {
      rsp.add(CommandOperation.ERR_MSGS, List.of("No " + key + " configured"));
    } else {
      rsp.add(key + ".enabled", getPlugin(key) != null);
      rsp.add(key, o);
    }
  }

  @Override
  protected boolean persistConf(SecurityConfig securityConfig) throws IOException {
    if (securityConfig == null || securityConfig.getData().isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed persisting security.json to SOLR_HOME. Object was empty.");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedData =
        (Map<String, Object>) Utils.getDeepCopy(securityConfig.getData(), 4);
    for (Object pluginConfig : sanitizedData.values()) {
      if (pluginConfig instanceof Map<?, ?> pluginConfigMap) {
        pluginConfigMap.remove("");
      }
    }
    try {
      replaceSecurityJson(Utils.toJSON(sanitizedData));
      if (log.isDebugEnabled()) {
        log.debug("Persisted security.json to {}", securityJsonPath);
      }
      return true;
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed persisting security.json to " + securityJsonPath,
          e);
    }
  }

  /**
   * Replaces security.json through a temporary file in the same directory. A symbolic link, a file
   * whose owner or permissions cannot carry over, and a file the directory cannot replace are
   * written in place.
   */
  private void replaceSecurityJson(byte[] json) throws IOException {
    if (!Files.isSymbolicLink(securityJsonPath)) {
      try {
        replaceThroughTemporaryFile(json);
        return;
      } catch (IOException e) {
        log.warn(
            "Could not replace {} through a temporary file, writing it in place",
            securityJsonPath,
            e);
      }
    }
    Files.write(securityJsonPath, json);
  }

  private void replaceThroughTemporaryFile(byte[] json) throws IOException {
    Path tmpPath =
        Files.createTempFile(
            securityJsonPath.getParent(), securityJsonPath.getFileName().toString(), ".tmp");
    try {
      Files.write(tmpPath, json);
      if (Files.exists(securityJsonPath)) {
        if (tmpPath.getFileSystem().supportedFileAttributeViews().contains("posix")) {
          Files.setPosixFilePermissions(tmpPath, Files.getPosixFilePermissions(securityJsonPath));
        }
        if (!Files.getOwner(tmpPath).equals(Files.getOwner(securityJsonPath))) {
          throw new IOException("security.json is owned by " + Files.getOwner(securityJsonPath));
        }
      }
      try {
        Files.move(
            tmpPath,
            securityJsonPath,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tmpPath, securityJsonPath, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      try {
        Files.deleteIfExists(tmpPath);
      } catch (IOException e) {
        log.warn("Could not remove temporary file {}", tmpPath, e);
      }
    }
  }

  @Override
  public String getDescription() {
    return "Edit or read security configuration locally in SOLR_HOME";
  }

  @Override
  protected void securityConfEdited() {
    // Need to call explicitly since we will not get notified of changes to local security.json
    cores.securityNodeChanged();
  }
}

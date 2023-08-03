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
package org.apache.solr.cloud;

import static org.apache.solr.common.params.CommonParams.NAME;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link OverseerMessageHandler} that handles ConfigSets API related overseer messages. */
public class OverseerConfigSetMessageHandler implements OverseerMessageHandler {

  /** Prefix to specify an action should be handled by this handler. */
  public static final String CONFIGSETS_ACTION_PREFIX = "configsets:";

  private ZkStateReader zkStateReader;

  private CoreContainer coreContainer;

  // we essentially implement a read/write lock for the ConfigSet exclusivity as follows:
  // WRITE: CREATE/DELETE on the ConfigSet under operation
  // READ: for the Base ConfigSet being copied in CREATE.
  // in this way, we prevent a Base ConfigSet from being deleted while it is being copied
  // but don't prevent different ConfigSets from being created with the same Base ConfigSet
  // at the same time.
  private final Set<String> configSetWriteWip;
  private final Set<String> configSetReadWip;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public OverseerConfigSetMessageHandler(ZkStateReader zkStateReader, CoreContainer coreContainer) {
    this.zkStateReader = zkStateReader;
    this.coreContainer = coreContainer;
    this.configSetWriteWip = new HashSet<>();
    this.configSetReadWip = new HashSet<>();
  }

  @Override
  public OverseerSolrResponse processMessage(ZkNodeProps message, String operation) {
    NamedList<Object> results = new NamedList<>();
    try {
      if (!operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Operation does not contain proper prefix: "
                + operation
                + " expected: "
                + CONFIGSETS_ACTION_PREFIX);
      }
      operation = operation.substring(CONFIGSETS_ACTION_PREFIX.length());
      log.info("OverseerConfigSetMessageHandler.processMessage : {}, {}", operation, message);

      ConfigSetParams.ConfigSetAction action = ConfigSetParams.ConfigSetAction.get(operation);
      if (action == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
      }
      switch (action) {
        case CREATE:
          ConfigSetCmds.createConfigSet(message, coreContainer);
          break;
        case DELETE:
          ConfigSetCmds.deleteConfigSet(message, coreContainer);
          break;
        default:
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
      }
    } catch (Exception e) {
      String configSetName = message.getStr(NAME);

      if (configSetName == null) {
        log.error("Operation {} failed", operation, e);
      } else {
        log.error("ConfigSet: {} operation: {} failed", configSetName, operation, e);
      }

      results.add("Operation " + operation + " caused exception:", e);
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException) e).code() : -1);
      results.add("exception", nl);
    }
    return new OverseerSolrResponse(results);
  }

  @Override
  public String getName() {
    return "Overseer ConfigSet Message Handler";
  }

  @Override
  public String getTimerName(String operation) {
    return "configset_" + operation;
  }

  @Override
  public Lock lockTask(ZkNodeProps message, long ignored) {
    String configSetName = getTaskKey(message);
    if (canExecute(configSetName, message)) {
      markExclusiveTask(configSetName, message);
      return () -> unmarkExclusiveTask(configSetName, message);
    }
    return null;
  }

  @Override
  public String getTaskKey(ZkNodeProps message) {
    return message.getStr(NAME);
  }

  private void markExclusiveTask(String configSetName, ZkNodeProps message) {
    String baseConfigSet = getBaseConfigSetIfCreate(message);
    markExclusive(configSetName, baseConfigSet);
  }

  private void markExclusive(String configSetName, String baseConfigSetName) {
    synchronized (configSetWriteWip) {
      configSetWriteWip.add(configSetName);
      if (baseConfigSetName != null) configSetReadWip.add(baseConfigSetName);
    }
  }

  private void unmarkExclusiveTask(String configSetName, ZkNodeProps message) {
    String baseConfigSet = getBaseConfigSetIfCreate(message);
    unmarkExclusiveConfigSet(configSetName, baseConfigSet);
  }

  private void unmarkExclusiveConfigSet(String configSetName, String baseConfigSetName) {
    synchronized (configSetWriteWip) {
      configSetWriteWip.remove(configSetName);
      if (baseConfigSetName != null) configSetReadWip.remove(baseConfigSetName);
    }
  }

  private boolean canExecute(String configSetName, ZkNodeProps message) {
    String baseConfigSetName = getBaseConfigSetIfCreate(message);

    synchronized (configSetWriteWip) {
      // need to acquire:
      // 1) write lock on ConfigSet
      // 2) read lock on Base ConfigSet
      if (configSetWriteWip.contains(configSetName) || configSetReadWip.contains(configSetName)) {
        return false;
      }
      if (baseConfigSetName != null && configSetWriteWip.contains(baseConfigSetName)) {
        return false;
      }
    }

    return true;
  }

  private String getBaseConfigSetIfCreate(ZkNodeProps message) {
    String operation =
        message.getStr(Overseer.QUEUE_OPERATION).substring(CONFIGSETS_ACTION_PREFIX.length());
    ConfigSetParams.ConfigSetAction action = ConfigSetParams.ConfigSetAction.get(operation);
    return ConfigSetCmds.getBaseConfigSetName(action, message.getStr(ConfigSetCmds.BASE_CONFIGSET));
  }
}

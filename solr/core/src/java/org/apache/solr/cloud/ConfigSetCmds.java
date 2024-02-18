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
import static org.apache.solr.common.params.ConfigSetParams.ConfigSetAction.CREATE;
import static org.apache.solr.common.util.Utils.toJSONString;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;

import com.jayway.jsonpath.internal.Utils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains methods dealing with Config Sets and called for Config Set API execution,
 * called from the {@link OverseerConfigSetMessageHandler} or from {@link
 * org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner#runConfigSetCommand}
 * depending on whether Collection and Config Set APIs are Overseer based or distributed.
 */
public class ConfigSetCmds {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Name of the ConfigSet to copy from for CREATE */
  public static final String BASE_CONFIGSET = "baseConfigSet";

  /** Prefix for properties that should be applied to the ConfigSet for CREATE */
  public static final String CONFIG_SET_PROPERTY_PREFIX = "configSetProp.";

  public static String getBaseConfigSetName(
      ConfigSetParams.ConfigSetAction action, String baseConfigSetName) {
    if (action == CREATE) {
      return Utils.isEmpty(baseConfigSetName) ? DEFAULT_CONFIGSET_NAME : baseConfigSetName;
    }
    return null;
  }

  private static NamedList<Object> getConfigSetProperties(
      ConfigSetService configSetService, String configName, String propertyPath)
      throws IOException {
    byte[] oldPropsData = configSetService.downloadFileFromConfig(configName, propertyPath);
    if (oldPropsData != null) {
      try (InputStreamReader reader =
          new InputStreamReader(new ByteArrayInputStream(oldPropsData), StandardCharsets.UTF_8)) {
        return ConfigSetProperties.readFromInputStream(reader);
      }
    }
    return null;
  }

  private static Map<String, Object> getNewProperties(ZkNodeProps message) {
    Map<String, Object> properties = null;
    for (Map.Entry<String, Object> entry : message.getProperties().entrySet()) {
      if (entry.getKey().startsWith(CONFIG_SET_PROPERTY_PREFIX)) {
        if (properties == null) {
          properties = new HashMap<>();
        }
        properties.put(
            entry.getKey().substring((CONFIG_SET_PROPERTY_PREFIX).length()), entry.getValue());
      }
    }
    return properties;
  }

  private static void mergeOldProperties(Map<String, Object> newProps, NamedList<Object> oldProps) {
    for (Map.Entry<String, Object> oldEntry : oldProps) {
      if (!newProps.containsKey(oldEntry.getKey())) {
        newProps.put(oldEntry.getKey(), oldEntry.getValue());
      }
    }
  }

  private static byte[] getPropertyData(Map<String, Object> newProps) {
    if (newProps != null) {
      String propertyDataStr = toJSONString(newProps);
      if (propertyDataStr == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Invalid property specification");
      }
      return propertyDataStr.getBytes(StandardCharsets.UTF_8);
    }
    return null;
  }

  public static void createConfigSet(ZkNodeProps message, CoreContainer coreContainer)
      throws IOException {
    String configSetName = message.getStr(NAME);
    if (configSetName == null || configSetName.length() == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "ConfigSet name not specified");
    }

    String baseConfigSetName = message.getStr(BASE_CONFIGSET, DEFAULT_CONFIGSET_NAME);

    if (coreContainer.getConfigSetService().checkConfigExists(configSetName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "ConfigSet already exists: " + configSetName);
    }

    // is there a base config that already exists
    if (!coreContainer.getConfigSetService().checkConfigExists(baseConfigSetName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Base ConfigSet does not exist: " + baseConfigSetName);
    }

    String propertyPath = ConfigSetProperties.DEFAULT_FILENAME;
    Map<String, Object> props = getNewProperties(message);
    if (props != null) {
      // read the old config properties and do a merge, if necessary
      NamedList<Object> oldProps =
          getConfigSetProperties(
              coreContainer.getConfigSetService(), baseConfigSetName, propertyPath);
      if (oldProps != null) {
        mergeOldProperties(props, oldProps);
      }
    }
    byte[] propertyData = getPropertyData(props);

    try {
      coreContainer.getConfigSetService().copyConfig(baseConfigSetName, configSetName);
      if (propertyData != null) {
        coreContainer
            .getConfigSetService()
            .uploadFileToConfig(configSetName, propertyPath, propertyData, true);
      }
    } catch (Exception e) {
      // copying the config dir or writing the properties file may have failed.
      // we should delete the ConfigSet because it may be invalid,
      // assuming we actually wrote something.  E.g. could be
      // the entire baseConfig set with the old properties, including immutable,
      // that would make it impossible for the user to delete.
      try {
        if (coreContainer.getConfigSetService().checkConfigExists(configSetName)) {
          deleteConfigSet(configSetName, true, coreContainer);
        }
      } catch (IOException ioe) {
        log.error("Error while trying to delete partially created ConfigSet", ioe);
      }
      throw e;
    }
  }

  public static void deleteConfigSet(ZkNodeProps message, CoreContainer coreContainer)
      throws IOException {
    String configSetName = message.getStr(NAME);
    if (configSetName == null || configSetName.length() == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "ConfigSet name not specified");
    }

    deleteConfigSet(configSetName, false, coreContainer);
  }

  private static void deleteConfigSet(
      String configSetName, boolean force, CoreContainer coreContainer) throws IOException {
    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();

    for (Map.Entry<String, DocCollection> entry :
        zkStateReader.getClusterState().getCollectionsMap().entrySet()) {
      String configName = entry.getValue().getConfigName();
      if (configSetName.equals(configName))
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Can not delete ConfigSet as it is currently being used by collection ["
                + entry.getKey()
                + "]");
    }

    String propertyPath = ConfigSetProperties.DEFAULT_FILENAME;
    NamedList<Object> properties =
        getConfigSetProperties(coreContainer.getConfigSetService(), configSetName, propertyPath);
    if (properties != null) {
      Object immutable = properties.get(ConfigSetProperties.IMMUTABLE_CONFIGSET_ARG);
      boolean isImmutableConfigSet =
          immutable != null ? Boolean.parseBoolean(immutable.toString()) : false;
      if (!force && isImmutableConfigSet) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Requested delete of immutable ConfigSet: " + configSetName);
      }
    }
    coreContainer.getConfigSetService().deleteConfig(configSetName);
  }
}

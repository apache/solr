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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.solr.api.ClusterPluginsSource;
import org.apache.solr.api.Command;
import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** API to maintain container-level plugin configurations. */
public class ContainerPluginsApi {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PLUGIN = "plugin";

  private final CoreContainer coreContainer;
  private final ClusterPluginsSource pluginsSource;

  public final Read readAPI = new Read();
  public final Edit editAPI = new Edit();

  public ContainerPluginsApi(
      CoreContainer coreContainer, ClusterPluginsSource clusterPluginsSource) {
    this.coreContainer = coreContainer;
    this.pluginsSource = clusterPluginsSource;
  }

  /** API for reading the current plugin configurations. */
  public class Read {
    @EndPoint(
        method = METHOD.GET,
        path = "/cluster/plugin",
        permission = PermissionNameProvider.Name.COLL_READ_PERM)
    public void list(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
      rsp.add(PLUGIN, pluginsSource.plugins());
    }
  }

  /** API for editing the plugin configurations. */
  @EndPoint(
      method = METHOD.POST,
      path = "/cluster/plugin",
      permission = PermissionNameProvider.Name.COLL_EDIT_PERM)
  public class Edit {

    @Command(name = "add")
    public void add(PayloadObj<PluginMeta> payload) throws IOException {
      PluginMeta info = payload.get();
      validateConfig(payload, info);
      if (payload.hasError()) return;
      pluginsSource.persistPlugins(
          map -> {
            if (map.containsKey(info.name)) {
              payload.addError(info.name + " already exists");
              return null;
            }
            map.put(info.name, payload.getDataMap());
            return map;
          });
    }

    @Command(name = "remove")
    public void remove(PayloadObj<String> payload) throws IOException {
      pluginsSource.persistPlugins(
          map -> {
            if (map.remove(payload.get()) == null) {
              payload.addError("No such plugin: " + payload.get());
              return null;
            }
            return map;
          });
    }

    @Command(name = "update")
    @SuppressWarnings("unchecked")
    public void update(PayloadObj<PluginMeta> payload) throws IOException {
      PluginMeta info = payload.get();
      validateConfig(payload, info);
      if (payload.hasError()) return;
      pluginsSource.persistPlugins(
          map -> {
            Map<String, Object> existing = (Map<String, Object>) map.get(info.name);
            if (existing == null) {
              payload.addError("No such plugin: " + info.name);
              return null;
            } else {
              Map<String, Object> jsonObj = payload.getDataMap();
              if (Objects.equals(jsonObj, existing)) {
                // no need to change anything
                return null;
              }
              map.put(info.name, jsonObj);
              return map;
            }
          });
    }
  }

  private void validateConfig(PayloadObj<PluginMeta> payload, PluginMeta info) throws IOException {
    if (info.klass.indexOf(':') > 0 && info.version == null) {
      payload.addError("Using package. must provide a packageVersion");
      return;
    }

    final List<String> errs = new ArrayList<>();
    final ContainerPluginsRegistry.ApiInfo apiInfo =
        coreContainer.getContainerPluginsRegistry().createInfo(payload.getDataMap(), errs);

    if (errs.isEmpty()) {
      try {
        apiInfo.init();
      } catch (Exception e) {
        log.error("Error instantiating plugin ", e);
        errs.add(e.getMessage());
      }
    }

    errs.forEach(payload::addError);
  }
}

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

package org.apache.solr.handler;

import com.google.common.collect.Maps;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.request.beans.ClusterPropPayload;
import org.apache.solr.client.solrj.request.beans.CreateConfigPayload;
import org.apache.solr.client.solrj.request.beans.RateLimiterPayload;
import org.apache.solr.cloud.ConfigSetCmds;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.*;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.REQUESTID;
import static org.apache.solr.common.params.CollectionParams.ACTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.*;
import static org.apache.solr.core.RateLimiterConfig.RL_CONFIG_KEY;
import static org.apache.solr.security.PermissionNameProvider.Name.*;

/**
 * All V2 APIs that have  a prefix of /api/cluster/
 */
public class ClusterAPI {
  private final CollectionsHandler collectionsHandler;
  private final ConfigSetsHandler configSetsHandler;

  public final Commands commands = new Commands();
  public final ConfigSetCommands configSetCommands = new ConfigSetCommands();

  public ClusterAPI(CollectionsHandler ch, ConfigSetsHandler configSetsHandler) {
    this.collectionsHandler = ch;
    this.configSetsHandler = configSetsHandler;
  }

  @EndPoint(method = GET,
          path = "/cluster/node-roles",
          permission = COLL_READ_PERM)
  public void roles(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.add("node-roles", readRecursive(ZkStateReader.NODE_ROLES,
            collectionsHandler.getCoreContainer().getZkController().getSolrCloudManager().getDistribStateManager(), 3));
  }

  Object readRecursive(String path, DistribStateManager zk, int depth) {
    if (depth == 0) return null;
    Map<String, Object> result;
    try {
      List<String> children = zk.listData(path);
      if (children != null && !children.isEmpty()) {
        result = new HashMap<>();
      } else {
        return Collections.emptySet();
      }
      for (String child: children) {
        Object c = readRecursive(path + "/" + child, zk, depth - 1);
        result.put(child, c);
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    if (depth == 1) {
      return result.keySet();
    } else {
      return result;
    }
  }

  @EndPoint(method = GET,
          path = "/cluster/node-roles/role/{role}",
          permission = COLL_READ_PERM)
  public void nodesWithRole(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String role = req.getPathTemplateValues().get("role");
    rsp.add("node-roles", Map.of(role,
            readRecursive(ZkStateReader.NODE_ROLES + "/" + role,
                    collectionsHandler.getCoreContainer().getZkController().getSolrCloudManager().getDistribStateManager(), 2)));
  }

  @EndPoint(method = GET,
          path = "/cluster/node-roles/node/{node}",
          permission = COLL_READ_PERM)
  @SuppressWarnings("unchecked")
  public void rolesForNode(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String node = req.getPathTemplateValues().get("node");
    Map<String, String> ret = new HashMap<String, String>();
    Map<String, Map<String, Set<String>>> roles = (Map<String, Map<String, Set<String>>>) readRecursive(ZkStateReader.NODE_ROLES,
            collectionsHandler.getCoreContainer().getZkController().getSolrCloudManager().getDistribStateManager(), 3);
    for (String role: roles.keySet()) {
      for (String mode: roles.get(role).keySet()) {
        if (roles.get(role).get(mode).isEmpty()) continue;
        Set<String> nodes = roles.get(role).get(mode);
        if (nodes.contains(node)) ret.put(role, mode);
      }
    }
    for (String role: ret.keySet()) {
      rsp.add(role, ret.get(role));
    }
  }

  @EndPoint(method = GET,
          path = "/cluster/node-roles/supported",
          permission = COLL_READ_PERM)
  public void supportedRoles(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
     Map<String, Object> roleModesSupportedMap = new HashMap<>();
    for (NodeRoles.Role role: NodeRoles.Role.values()) {
      roleModesSupportedMap.put(role.toString(),
              Map.of("modes", role.supportedModes()));
    }
    rsp.add("supported-roles", roleModesSupportedMap);
  }

  @EndPoint(method = GET,
          path = "/cluster/node-roles/role/{role}/{mode}",
          permission = COLL_READ_PERM)
  public void nodesWithRoleMode(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Here, deal with raw strings instead of Role & Mode types so as to handle roles and modes
    // that are not understood by this node (possibly during a rolling upgrade)
    String roleStr = req.getPathTemplateValues().get("role");
    String modeStr = req.getPathTemplateValues().get("mode");

    List<String> nodes =  collectionsHandler.getCoreContainer().getZkController().getSolrCloudManager()
            .getDistribStateManager().listData(ZkStateReader.NODE_ROLES + "/" + roleStr + "/" + modeStr);
    rsp.add( "node-roles", Map.of(roleStr, Collections.singletonMap(modeStr, nodes)));
  }

   public static List<String> getNodesByRole(NodeRoles.Role role, String mode, DistribStateManager zk)
          throws InterruptedException, IOException, KeeperException {
    try {
      return zk.listData(ZkStateReader.NODE_ROLES + "/" + role + "/" + mode);
    } catch (NoSuchElementException e) {
      return Collections.emptyList();
    }
  }


  @EndPoint(method = GET,
          path = "/cluster/aliases",
          permission = COLL_READ_PERM)
  public void aliases(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final Map<String, Object> v1Params = Maps.newHashMap();
    v1Params.put(ACTION, CollectionParams.CollectionAction.LISTALIASES.lowerName);
    collectionsHandler.handleRequestBody(wrapParams(req, v1Params), rsp);
  }

  @EndPoint(method = GET,
          path = "/cluster/overseer",
          permission = COLL_READ_PERM)
  public void getOverseerStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    collectionsHandler.handleRequestBody(wrapParams(req, "action", OVERSEERSTATUS.lowerName), rsp);
  }

  @EndPoint(method = GET,
          path = "/cluster",
          permission = COLL_READ_PERM)
  public void getCluster(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    collectionsHandler.handleRequestBody(wrapParams(req, "action", LIST.lowerName), rsp);
  }

  @EndPoint(method = DELETE,
          path = "/cluster/command-status/{id}",
          permission = COLL_EDIT_PERM)
  public void deleteCommandStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final Map<String, Object> v1Params = Maps.newHashMap();
    v1Params.put(ACTION, DELETESTATUS.lowerName);
    v1Params.put(REQUESTID, req.getPathTemplateValues().get("id"));
    collectionsHandler.handleRequestBody(wrapParams(req, v1Params), rsp);
  }

  @EndPoint(method = DELETE,
          path = "/cluster/command-status",
          permission = COLL_EDIT_PERM)
  public void flushCommandStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CollectionsHandler.CollectionOperation.DELETESTATUS_OP.execute(req, rsp, collectionsHandler);
  }

  @EndPoint(method = DELETE,
          path = "/cluster/configs/{name}",
          permission = CONFIG_EDIT_PERM
  )
  public void deleteConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = wrapParams(req,
            "action", ConfigSetParams.ConfigSetAction.DELETE.toString(),
            CommonParams.NAME, req.getPathTemplateValues().get("name"));
    configSetsHandler.handleRequestBody(req, rsp);
  }

  @EndPoint(method = GET,
          path = "/cluster/configs",
          permission = CONFIG_READ_PERM)
  public void listConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = wrapParams(req, "action", ConfigSetParams.ConfigSetAction.LIST.toString());
    configSetsHandler.handleRequestBody(req, rsp);
  }

  @EndPoint(method = POST,
          path = "/cluster/configs",
          permission = CONFIG_EDIT_PERM
  )
  public class ConfigSetCommands {

    @Command(name = "create")
    @SuppressWarnings("unchecked")
    public void create(PayloadObj<CreateConfigPayload> obj) throws Exception {
      Map<String, Object> mapVals = obj.get().toMap(new HashMap<>());
      Map<String, Object> customProps = obj.get().properties;
      if (customProps != null) {
        customProps.forEach((k, o) -> mapVals.put(ConfigSetCmds.CONFIG_SET_PROPERTY_PREFIX + k, o));
      }
      mapVals.put("action", ConfigSetParams.ConfigSetAction.CREATE.toString());
      configSetsHandler.handleRequestBody(wrapParams(obj.getRequest(), mapVals), obj.getResponse());
    }

  }

  @EndPoint(method = PUT,
          path = "/cluster/configs/{name}",
          permission = CONFIG_EDIT_PERM
  )
  public void uploadConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = wrapParams(req,
            "action", ConfigSetParams.ConfigSetAction.UPLOAD.toString(),
            CommonParams.NAME, req.getPathTemplateValues().get("name"),
            ConfigSetParams.OVERWRITE, true,
            ConfigSetParams.CLEANUP, false);
    configSetsHandler.handleRequestBody(req, rsp);
  }

  @EndPoint(method = PUT,
          path = "/cluster/configs/{name}/*",
          permission = CONFIG_EDIT_PERM
  )
  public void insertIntoConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String path = req.getPathTemplateValues().get("*");
    if (path == null || path.isBlank()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "In order to insert a file in a configSet, a filePath must be provided in the url after the name of the configSet.");
    }
    req = wrapParams(req, Map.of("action", ConfigSetParams.ConfigSetAction.UPLOAD.toString(),
            CommonParams.NAME, req.getPathTemplateValues().get("name"),
            ConfigSetParams.FILE_PATH, path,
            ConfigSetParams.OVERWRITE, true,
            ConfigSetParams.CLEANUP, false));
    configSetsHandler.handleRequestBody(req, rsp);
  }

  public static SolrQueryRequest wrapParams(SolrQueryRequest req, Object... def) {
    Map<String, Object> m = Utils.makeMap(def);
    return wrapParams(req, m);
  }

  public static SolrQueryRequest wrapParams(SolrQueryRequest req, Map<String, Object> m) {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    m.forEach((k, v) -> {
      if (v == null) return;
      if (v instanceof String[]) {
        solrParams.add(k, (String[]) v);
      } else {
        solrParams.add(k, String.valueOf(v));
      }
    });
    DefaultSolrParams dsp = new DefaultSolrParams(req.getParams(), solrParams);
    req.setParams(dsp);
    return req;
  }

  @EndPoint(method = GET,
          path = "/cluster/command-status/{id}",
          permission = COLL_READ_PERM)
  public void getCommandStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final Map<String, Object> v1Params = Maps.newHashMap();
    v1Params.put(ACTION, REQUESTSTATUS.lowerName);
    v1Params.put(REQUESTID, req.getPathTemplateValues().get("id"));
    collectionsHandler.handleRequestBody(wrapParams(req, v1Params), rsp);
  }

  @EndPoint(method = GET,
          path = "/cluster/nodes",
          permission = COLL_READ_PERM)
  public void getNodes(SolrQueryRequest req, SolrQueryResponse rsp) {
    rsp.add("nodes", getCoreContainer().getZkController().getClusterState().getLiveNodes());
  }

  private CoreContainer getCoreContainer() {
    return collectionsHandler.getCoreContainer();
  }

  @EndPoint(method = POST,
          path = "/cluster",
          permission = COLL_EDIT_PERM)
  public class Commands {
    @Command(name = "add-role")
    public void addRole(PayloadObj<RoleInfo> obj) throws Exception {
      RoleInfo info = obj.get();
      Map<String, Object> m = info.toMap(new HashMap<>());
      m.put("action", ADDROLE.toString());
      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), m), obj.getResponse());
    }

    @Command(name = "remove-role")
    public void removeRole(PayloadObj<RoleInfo> obj) throws Exception {
      RoleInfo info = obj.get();
      Map<String, Object> m = info.toMap(new HashMap<>());
      m.put("action", REMOVEROLE.toString());
      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), m), obj.getResponse());
    }

    @Command(name = "set-obj-property")
    public void setObjProperty(PayloadObj<ClusterPropPayload> obj) {
      //Not using the object directly here because the API differentiate between {name:null} and {}
      Map<String, Object> m = obj.getDataMap();
      ClusterProperties clusterProperties = new ClusterProperties(getCoreContainer().getZkController().getZkClient());
      try {
        clusterProperties.setClusterProperties(m);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
      }
    }

    @Command(name = "set-property")
    public void setProperty(PayloadObj<Map<String, String>> obj) throws Exception {
      Map<String, Object> m = obj.getDataMap();
      m.put("action", CLUSTERPROP.toString());
      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), m), obj.getResponse());
    }

    @Command(name = "set-ratelimiter")
    public void setRateLimiters(PayloadObj<RateLimiterPayload> payLoad) {
      RateLimiterPayload rateLimiterConfig = payLoad.get();
      ClusterProperties clusterProperties = new ClusterProperties(getCoreContainer().getZkController().getZkClient());

      try {
        clusterProperties.update(rateLimiterConfig, RL_CONFIG_KEY);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
      }
    }
  }

  public static class RoleInfo implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String node;
    @JsonProperty(required = true)
    public String role;

  }


}

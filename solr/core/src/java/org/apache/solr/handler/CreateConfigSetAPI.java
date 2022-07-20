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
import org.apache.commons.collections4.MapUtils;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.beans.CreateConfigPayload;
import org.apache.solr.cloud.ConfigSetCmds;
import org.apache.solr.cloud.OverseerSolrResponseSerializer;
import org.apache.solr.cloud.OverseerTaskQueue;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.CONFIGSETS_ACTION_PREFIX;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.admin.ConfigSetsHandler.CONFIG_SET_TIMEOUT;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DISABLE_CREATE_AUTH_CHECKS;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

/**
 * V2 API to create a new configset based on an existing one.
 */
@EndPoint(method = POST, path = "/cluster/configs", permission = CONFIG_EDIT_PERM)
public class CreateConfigSetAPI {

    private final ConfigSetsHandler configSetsHandler;
    private final CoreContainer coreContainer;
    private final ConfigSetService configSetService;
    private final Optional<DistributedCollectionConfigSetCommandRunner>
            distributedCollectionConfigSetCommandRunner;

    public CreateConfigSetAPI(ConfigSetsHandler configSetsHandler, CoreContainer coreContainer) {
        this.configSetsHandler = configSetsHandler;
        this.coreContainer = coreContainer;
        this.configSetService = coreContainer.getConfigSetService();
        this.distributedCollectionConfigSetCommandRunner = coreContainer.getDistributedCollectionCommandRunner();
    }

    @Command(name = "create")
    @SuppressWarnings("unchecked")
    public void create(PayloadObj<CreateConfigPayload> obj) throws Exception {
        final CreateConfigPayload createConfigPayload = obj.get();
        final String baseConfigSetName = (createConfigPayload.baseConfigSet != null) ? createConfigPayload.baseConfigSet : DEFAULT_CONFIGSET_NAME;
        if (configSetService.checkConfigExists(createConfigPayload.name)) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST, "ConfigSet already exists: " + createConfigPayload.name);
        }

        // is there a base config that already exists
        if (! configSetService.checkConfigExists(baseConfigSetName)) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST, "Base ConfigSet does not exist: " + baseConfigSetName);
        }

        if (!DISABLE_CREATE_AUTH_CHECKS
                && !isTrusted(obj.getRequest().getUserPrincipal(), coreContainer.getAuthenticationPlugin())
                && isCurrentlyTrusted(baseConfigSetName)) {
            throw new SolrException(
                    SolrException.ErrorCode.UNAUTHORIZED,
                    "Can't create a configset with an unauthenticated request from a trusted "
                            + ConfigSetCmds.BASE_CONFIGSET);
        }

        final Map<String, Object> overseerMessage = Maps.newHashMap();
        overseerMessage.put(NAME, createConfigPayload.name);
        overseerMessage.put(ConfigSetCmds.BASE_CONFIGSET, baseConfigSetName);
        if (! MapUtils.isEmpty(createConfigPayload.properties)) {
            for (Map.Entry<String, Object> e : createConfigPayload.properties.entrySet()) {
                overseerMessage.put(ConfigSetCmds.CONFIG_SET_PROPERTY_PREFIX + e.getKey(), e.getValue());
            }
        }

        runConfigSetCommand(obj.getResponse(), ConfigSetsHandler.ConfigSetOperation.CREATE_OP, overseerMessage);
    }

    private void runConfigSetCommand(SolrQueryResponse rsp, ConfigSetsHandler.ConfigSetOperation operation,
                                     Map<String, Object> messageToSend) throws Exception {
        if (distributedCollectionConfigSetCommandRunner.isPresent()) {
            distributedCollectionConfigSetCommandRunner
                    .get()
                    .runConfigSetCommand(rsp, operation, messageToSend, CONFIG_SET_TIMEOUT);
        } else {
            sendToOverseer(rsp, operation, messageToSend);
        }
    }

    protected void sendToOverseer(
            SolrQueryResponse rsp, ConfigSetsHandler.ConfigSetOperation operation, Map<String, Object> result)
            throws KeeperException, InterruptedException {
        // We need to differentiate between collection and configsets actions since they currently
        // use the same underlying queue.
        result.put(QUEUE_OPERATION, CONFIGSETS_ACTION_PREFIX + operation.getAction().toLower());
        ZkNodeProps props = new ZkNodeProps(result);
        handleResponse(operation.getAction().toLower(), props, rsp, CONFIG_SET_TIMEOUT);
    }

    private void handleResponse(String operation, ZkNodeProps m, SolrQueryResponse rsp, long timeout)
            throws KeeperException, InterruptedException {
        long time = System.nanoTime();

        OverseerTaskQueue.QueueEvent event =
                coreContainer.getZkController().getOverseerConfigSetQueue().offer(Utils.toJSON(m), timeout);
        if (event.getBytes() != null) {
            SolrResponse response = OverseerSolrResponseSerializer.deserialize(event.getBytes());
            rsp.getValues().addAll(response.getResponse());
            SimpleOrderedMap<?> exp = (SimpleOrderedMap<?>) response.getResponse().get("exception");
            if (exp != null) {
                Integer code = (Integer) exp.get("rspCode");
                rsp.setException(
                        new SolrException(
                                code != null && code != -1 ? SolrException.ErrorCode.getErrorCode(code) : SolrException.ErrorCode.SERVER_ERROR,
                                (String) exp.get("msg")));
            }
        } else {
            if (System.nanoTime() - time
                    >= TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS)) {
                throw new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR, operation + " the configset time out:" + timeout / 1000 + "s");
            } else if (event.getWatchedEvent() != null) {
                throw new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        operation
                                + " the configset error [Watcher fired on path: "
                                + event.getWatchedEvent().getPath()
                                + " state: "
                                + event.getWatchedEvent().getState()
                                + " type "
                                + event.getWatchedEvent().getType()
                                + "]");
            } else {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, operation + " the configset unknown case");
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private boolean isTrusted(Principal userPrincipal, AuthenticationPlugin authPlugin) {
        if (authPlugin != null && userPrincipal != null) {
            log.debug("Trusted configset request");
            return true;
        }
        log.debug("Untrusted configset request");
        return false;
    }

    private boolean isCurrentlyTrusted(String configName)
            throws IOException {
        Map<String, Object> contentMap = configSetService.getConfigMetadata(configName);
        return (boolean) contentMap.getOrDefault("trusted", true);
    }
}

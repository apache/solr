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
package org.apache.solr.handler.admin.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.security.PermissionNameProvider.Name.ZK_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.ZK_READ_PERM;

import jakarta.inject.Inject;
import java.util.ArrayList;
import org.apache.solr.client.api.endpoint.ZooKeeperEnsembleApis;
import org.apache.solr.client.api.model.ZooKeeperEnsembleResponse;
import org.apache.solr.client.api.model.ZooKeeperMember;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkDynamicConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;

/** V2 access to ZooKeeper's versioned dynamic ensemble configuration. */
public class ZooKeeperEnsemble extends AdminAPIBase implements ZooKeeperEnsembleApis {
  @Inject
  public ZooKeeperEnsemble(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer, req, rsp);
  }

  @Override
  @PermissionName(ZK_READ_PERM)
  public ZooKeeperEnsembleResponse listZooKeeperMembers() {
    var zkClient = zkClient();
    try {
      return response(readConfig(zkClient));
    } catch (Exception e) {
      throw translate(e);
    }
  }

  @Override
  @PermissionName(ZK_EDIT_PERM)
  public ZooKeeperEnsembleResponse deleteZooKeeperMember(String memberId, String version) {
    var zkClient = zkClient();
    final int id;
    final Long requestedVersion;
    try {
      id = Integer.parseInt(memberId);
      if (id <= 0) throw new NumberFormatException();
      requestedVersion = version == null ? null : Long.parseUnsignedLong(version, 16);
      if (version != null && !version.matches("[0-9a-fA-F]+")) throw new NumberFormatException();
    } catch (NumberFormatException e) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST, "Expected a positive member ID and hexadecimal version", e);
    }
    try {
      var current = readConfig(zkClient);
      long expectedVersion = Long.parseUnsignedLong(current.getVersion(), 16);
      if (requestedVersion != null && requestedVersion != expectedVersion) {
        throw new SolrException(ErrorCode.CONFLICT, "ZooKeeper membership version has changed");
      }
      if (current.getServers().stream().noneMatch(s -> s.serverId == id)) {
        throw new SolrException(ErrorCode.NOT_FOUND, "No ZooKeeper member with ID " + id);
      }
      byte[] updated =
          zkClient
              .getCuratorFramework()
              .reconfig()
              .leaving(Integer.toString(id))
              .fromConfig(expectedVersion)
              .forEnsemble();
      return response(ZkDynamicConfig.parseLines(new String(updated, UTF_8)));
    } catch (Exception e) {
      throw translate(e);
    }
  }

  private SolrZkClient zkClient() {
    validateZooKeeperAwareCoreContainer(coreContainer);
    disableResponseCaching();
    return coreContainer.getZkController().getZkClient();
  }

  private ZkDynamicConfig readConfig(SolrZkClient zkClient) throws Exception {
    var config =
        ZkDynamicConfig.parseLines(
            new String(zkClient.getCuratorFramework().getConfig().forEnsemble(), UTF_8));
    if (config.getServers().isEmpty() || config.getVersion().isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "ZooKeeper is not configured as an ensemble");
    }
    return config;
  }

  private ZooKeeperEnsembleResponse response(ZkDynamicConfig config) {
    var result = instantiateJerseyResponse(ZooKeeperEnsembleResponse.class);
    result.version = config.getVersion();
    result.members = new ArrayList<>();
    for (var server : config.getServers()) {
      var member = new ZooKeeperMember();
      member.id = server.serverId;
      member.host = server.resolveClientPortAddress();
      member.clientPort = server.clientPort;
      member.quorumPort = server.leaderPort;
      member.electionPort = server.leaderElectionPort;
      member.role = server.role == null ? "participant" : server.role;
      result.members.add(member);
    }
    return result;
  }

  private SolrException translate(Exception e) {
    if (e instanceof SolrException se) return se;
    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
    ErrorCode code = ErrorCode.SERVICE_UNAVAILABLE;
    if (e instanceof KeeperException ke) {
      code =
          switch (ke.code()) {
            case BADARGUMENTS, RECONFIGDISABLED, NONODE -> ErrorCode.BAD_REQUEST;
            case BADVERSION, RECONFIGINPROGRESS -> ErrorCode.CONFLICT;
            case NOAUTH, AUTHFAILED -> ErrorCode.FORBIDDEN;
            default -> ErrorCode.SERVICE_UNAVAILABLE;
          };
    }
    return new SolrException(code, "ZooKeeper ensemble operation failed: " + e.getMessage(), e);
  }
}

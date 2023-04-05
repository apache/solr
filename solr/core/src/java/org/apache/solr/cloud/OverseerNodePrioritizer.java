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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.impl.ZkDistribStateManager;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.handler.ClusterAPI;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for prioritization of Overseer nodes, for example with the ADDROLE collection
 * command.
 */
public class OverseerNodePrioritizer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ZkStateReader zkStateReader;
  private final String adminPath;
  private final ShardHandlerFactory shardHandlerFactory;

  /** Only used to send QUIT to the overseer */
  private final Overseer overseer;

  public OverseerNodePrioritizer(
      ZkStateReader zkStateReader,
      Overseer overseer,
      String adminPath,
      ShardHandlerFactory shardHandlerFactory) {
    this.zkStateReader = zkStateReader;
    this.adminPath = adminPath;
    this.shardHandlerFactory = shardHandlerFactory;
    this.overseer = overseer;
  }

  public synchronized void prioritizeOverseerNodes(String overseerId) throws Exception {
    SolrZkClient zk = zkStateReader.getZkClient();
    List<String> overseerDesignates = new ArrayList<>();
    if (zk.exists(ZkStateReader.ROLES, true)) {
      Map<?, ?> m =
          (Map<?, ?>) Utils.fromJSON(zk.getData(ZkStateReader.ROLES, null, new Stat(), true));
      @SuppressWarnings("unchecked")
      List<String> l = (List<String>) m.get("overseer");
      if (l != null) {
        overseerDesignates.addAll(l);
      }
    }

    List<String> preferredOverseers =
        ClusterAPI.getNodesByRole(
            NodeRoles.Role.OVERSEER,
            NodeRoles.MODE_PREFERRED,
            new ZkDistribStateManager(zkStateReader.getZkClient()));
    for (String preferred : preferredOverseers) {
      if (overseerDesignates.contains(preferred)) {
        log.warn(
            "Node {} has been configured to be a preferred overseer using both ADDROLE API command "
                + "as well as using Node Roles (i.e. -Dsolr.node.roles start up property). Only the latter is recommended.",
            preferred);
      }
    }
    overseerDesignates.addAll(preferredOverseers);
    if (overseerDesignates.isEmpty()) return;
    String ldr = OverseerTaskProcessor.getLeaderNode(zk);
    if (overseerDesignates.contains(ldr)) return;
    log.info(
        "prioritizing overseer nodes at {} overseer designates are {}",
        overseerId,
        overseerDesignates);
    List<String> electionNodes =
        OverseerTaskProcessor.getSortedElectionNodes(
            zk, Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE);
    if (electionNodes.size() < 2) return;
    log.info("sorted nodes {}", electionNodes);

    String designateNodeId = null;
    for (String electionNode : electionNodes) {
      if (overseerDesignates.contains(LeaderElector.getNodeName(electionNode))) {
        designateNodeId = electionNode;
        break;
      }
    }

    if (designateNodeId == null) {
      log.warn("No live overseer designate ");
      return;
    }
    if (!designateNodeId.equals(electionNodes.get(1))) { // checking if it is already at no:1
      log.info("asking node {} to come join election at head", designateNodeId);
      invokeOverseerOpWithRetries(
          designateNodeId, "rejoinAtHead", 5); // ask designate to come first
      if (log.isInfoEnabled()) {
        log.info("asking the old first in line {} to rejoin election  ", electionNodes.get(1));
      }
      invokeOverseerOpWithRetries(
          electionNodes.get(1), "rejoin", 5); // ask second inline to go behind
      if (log.isInfoEnabled()) {
        List<String> newElectionNodes =
            OverseerTaskProcessor.getSortedElectionNodes(
                zk, Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE);
        log.info("sorted nodes after prioritization {}", newElectionNodes);
      }
    }
    // now ask the current leader to QUIT , so that the designate can takeover
    overseer.sendQuitToOverseer(OverseerTaskProcessor.getLeaderId(zkStateReader.getZkClient()));
  }

  private void invokeOverseerOpWithRetries(String electionNode, String op, int retryCount) {
    boolean successful = false;
    for (int i = 0; i < retryCount && !successful; i++) {
      try {
        invokeOverseerOp(electionNode, op);
        successful = true;
      } catch (SolrException e) {
        if (i < retryCount - 1) {
          log.warn("Exception occurred while invoking Overseer Operation '{}'. Retrying.", op, e);
          try {
            Thread.sleep(100);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            break;
          }
        } else {
          throw e;
        }
      }
    }
  }

  private void invokeOverseerOp(String electionNode, String op) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.OVERSEEROP.toString());
    params.set("op", op);
    params.set("qt", adminPath);
    params.set("electionNode", electionNode);
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
    String replica = zkStateReader.getBaseUrlForNodeName(LeaderElector.getNodeName(electionNode));
    sreq.shards = new String[] {replica};
    sreq.actualShards = sreq.shards;
    sreq.params = params;
    shardHandler.submit(sreq, replica, sreq.params);
    ShardResponse response = shardHandler.takeCompletedOrError();
    if (response.getException() != null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Exception occurred while invoking Overseer Operation: " + op,
          response.getException());
    }
  }
}

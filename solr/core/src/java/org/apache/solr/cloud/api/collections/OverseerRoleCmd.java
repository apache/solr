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

package org.apache.solr.cloud.api.collections;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.cloud.OverseerNodePrioritizer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverseerRoleCmd implements CollApiCmds.CollectionApiCommand {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CollectionCommandContext ccc;
  private final CollectionAction operation;
  private final OverseerNodePrioritizer overseerPrioritizer;

  public OverseerRoleCmd(
      CollectionCommandContext ccc,
      CollectionAction operation,
      OverseerNodePrioritizer prioritizer) {
    this.ccc = ccc;
    this.operation = operation;
    this.overseerPrioritizer = prioritizer;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    if (ccc.isDistributedCollectionAPI()) {
      // No Overseer (not accessible from Collection API command execution in any case) so this
      // command can't be run...
      log.error(
          "Cluster is running with distributed Collection API execution. Ignoring Collection API operation "
              + operation); // nowarn
      return;
    }
    ZkStateReader zkStateReader = ccc.getZkStateReader();
    SolrZkClient zkClient = zkStateReader.getZkClient();
    Map<String, List<String>> roles = null;
    String node = message.getStr("node");
    if ("false".equals(message.getStr("persist"))) { // no need to persist to roles.json
      runPrioritizer();
      return;
    }

    String roleName = message.getStr("role");
    boolean nodeExists = zkClient.exists(ZkStateReader.ROLES, true);
    if (nodeExists) {
      @SuppressWarnings("unchecked")
      Map<String, List<String>> tmp =
          (Map<String, List<String>>)
              Utils.fromJSON(zkClient.getData(ZkStateReader.ROLES, null, new Stat(), true));
      roles = tmp;
    } else {
      roles = CollectionUtil.newLinkedHashMap(1);
    }

    List<String> nodeList = roles.computeIfAbsent(roleName, k -> new ArrayList<>());
    if (ADDROLE == operation) {
      log.info("Overseer role added to {}", node);
      if (!nodeList.contains(node)) nodeList.add(node);
    } else if (REMOVEROLE == operation) {
      log.info("Overseer role removed from {}", node);
      nodeList.remove(node);
    }

    if (nodeExists) {
      zkClient.setData(ZkStateReader.ROLES, Utils.toJSON(roles), true);
    } else {
      zkClient.create(ZkStateReader.ROLES, Utils.toJSON(roles), CreateMode.PERSISTENT, true);
    }
    runPrioritizer();
  }

  private void runPrioritizer() {
    // if there are too many nodes this command may time out. And most likely dedicated
    // overseers are created when there are too many nodes  . So , do this operation in a separate
    // thread
    new Thread(
            () -> {
              try {
                overseerPrioritizer.prioritizeOverseerNodes(ccc.getOverseerId());
              } catch (Exception e) {
                log.error("Error in prioritizing Overseer", e);
              }
            },
            "OverseerPrioritizationThread")
        .start();
  }
}

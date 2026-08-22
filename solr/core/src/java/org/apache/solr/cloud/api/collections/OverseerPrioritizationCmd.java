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

import java.lang.invoke.MethodHandles;
import org.apache.solr.cloud.OverseerNodePrioritizer;
import org.apache.solr.cloud.api.collections.CollApiCmds.CollectionApiCommand;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Internal message asking the Overseer to re-run overseer-node prioritization. */
public class OverseerPrioritizationCmd implements CollectionApiCommand {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CollectionCommandContext ccc;
  private final OverseerNodePrioritizer overseerPrioritizer;

  public OverseerPrioritizationCmd(
      CollectionCommandContext ccc, OverseerNodePrioritizer prioritizer) {
    this.ccc = ccc;
    this.overseerPrioritizer = prioritizer;
  }

  @Override
  public void call(AdminCmdContext context, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    if (ccc.isDistributedCollectionAPI()) {
      // No Overseer (not accessible from Collection API command execution in any case) so this
      // command can't be run...
      log.error(
          "Cluster is running with distributed Collection API execution. Ignoring internal overseer"
              + " prioritization request.");
      return;
    }
    // if there are too many nodes this may time out, and dedicated overseers are most likely
    // configured when there are many nodes, so do it in a separate thread
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

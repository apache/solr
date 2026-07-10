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

import static org.apache.solr.common.params.CommonParams.ID;

import java.lang.invoke.MethodHandles;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OverseerElectionContext extends ElectionContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrZkClient zkClient;
  private final Overseer overseer;
  private volatile boolean isClosed = false;

  public OverseerElectionContext(
      SolrZkClient zkClient, Overseer overseer, final String zkNodeName) {
    super(zkNodeName, Overseer.OVERSEER_ELECT, Overseer.OVERSEER_ELECT + "/leader", null, zkClient);
    this.overseer = overseer;
    this.zkClient = zkClient;
    try {
      ZkMaintenanceUtils.ensureExists(Overseer.OVERSEER_ELECT, zkClient);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  void runLeaderProcess(boolean weAreReplacement) throws KeeperException, InterruptedException {
    if (isClosed) {
      return;
    }
    log.info("I am going to be the leader {}", id);
    final String id = leaderSeqPath.substring(leaderSeqPath.lastIndexOf('/') + 1);
    ZkNodeProps myProps = new ZkNodeProps(ID, id);

    // Create the leader registration znode and start the overseer atomically, under the same lock
    // close() takes, so a concurrent close() can no longer slip in between the create and start and
    // leave a "zombie leader" znode with no overseer behind it. The registration is a multi op that
    // also setData's the parent, which bumps (and lets us capture) the parent version so
    // cancelElection() can later delete only our own registration, ABA-safe. Mirrors
    // ShardLeaderElectionContextBase.
    synchronized (this) {
      boolean shutDown = overseer.getZkController().getCoreContainer().isShutDown();
      if (!this.isClosed && !shutDown) {
        registerLeaderNode(Utils.toJSON(myProps));
        log.info("Created overseer leader registration {} -> {}", leaderPath, id);
        overseer.start(id);
      } else {
        Thread.dumpStack();
        log.warn(
            "ZOMBIE LEADER: created leader registration {} -> {} but skipping overseer.start() "
                + "because the election context was closed underneath us (isClosed={}, shutDown={}). "
                + "The leader znode now points at an overseer that will never run and will not be "
                + "removed by cancelElection(); subsequent elections will fail with NodeExists.",
            leaderPath,
            id,
            this.isClosed,
            shutDown);
      }
    }
  }

  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
    // Delete our own leader registration, guarded by the parent version we captured at
    // registration,
    // so we can never delete a newer lineage's/host's registration (ABA-safe). Mirrors
    // ShardLeaderElectionContextBase.cancelElection.
    synchronized (this) {
      if (leaderZkNodeParentVersion != null) {
        try {
          deleteLeaderNode();
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
          // A newer lineage already re-registered (parent version bumped) or the node is already
          // gone -- either way the leader znode is not ours to remove.
        }
        leaderZkNodeParentVersion = null;
      }
    }
    overseer.close();
  }

  @Override
  public synchronized void close() {
    Thread.dumpStack();
    this.isClosed = true;
    overseer.close();
  }

  @Override
  public ElectionContext copy() {
    return new OverseerElectionContext(zkClient, overseer, id);
  }

  @Override
  public void joinedElectionFired() {
    overseer.close();
  }

  @Override
  public void checkIfIamLeaderFired() {
    // leader changed - close the overseer
    overseer.close();
  }
}

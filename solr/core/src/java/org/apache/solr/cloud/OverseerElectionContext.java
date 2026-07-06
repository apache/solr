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
import org.apache.zookeeper.CreateMode;
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

    // NOTE: this create of the leader registration znode happens OUTSIDE the synchronized(this)
    // guard below. isClosed is only checked at the top of this method and again before start();
    // if another thread closes this context between here and the guard, we will have created the
    // leader znode with no overseer running behind it (a "zombie leader") that cancelElection()
    // does not clean up.
    zkClient.makePath(leaderPath, Utils.toJSON(myProps), CreateMode.EPHEMERAL);
    log.info("Created overseer leader registration {} -> {}", leaderPath, id);

    synchronized (this) {
      boolean shutDown = overseer.getZkController().getCoreContainer().isShutDown();
      if (!this.isClosed && !shutDown) {
        overseer.start(id);
      } else {
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
    overseer.close();
  }

  @Override
  public synchronized void close() {
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

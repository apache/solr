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

import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE_STATUS;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class PrepRecoveryOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CallInfo it) throws Exception {

    final SolrParams params = it.req.getParams();

    String cname = params.required().get(CoreAdminParams.CORE);

    String leaderName = params.required().get("leaderName");

    String collection = params.get("coresCollection");

    String state = params.get(ZkStateReader.STATE_PROP);

    boolean checkIsLeader = params.getBool("checkIsLeader", false);


    Replica.State waitForState = null;
    if (state != null) {
      waitForState = Replica.State.getState(state);
    }

    if (checkIsLeader) {
      LeaderElector leaderElector = it.handler.coreContainer.getZkController().getLeaderElector(leaderName);
      if (leaderElector == null || !leaderElector.isLeader()) {
        log.info("checkValidLeader failed for {} {} state={} params={}", leaderName, leaderElector, leaderElector == null ? "(null elector)" : leaderElector.getState(), params);
        it.rsp.add(RESPONSE_STATUS, "1");
        return;
      }
    }

    if (waitForState == null) {
      it.rsp.add(RESPONSE_STATUS, "0");
      return;
    }

    assert TestInjection.injectPrepRecoveryOpPauseForever();

    CoreContainer coreContainer = it.handler.coreContainer;

    try {
      log.info("going to wait for core: {}, state: {}: params={}", cname, waitForState, params);
      Replica.State finalWaitForState = waitForState;
      coreContainer.getZkController().getZkStateReader().waitForState(collection, 5, TimeUnit.SECONDS, (n, c) -> {
        if (c == null) {
          return false;
        }

        // wait until we are sure the recovering node is ready
        // to accept updates
        final Replica replica = c.getReplica(cname);
        boolean isLive = false;
        if (replica == null) {
          log.info("replica not found={}", cname);
          return false;
        }

        // if we look active, we have been getting updates
        if (replica.getState() == finalWaitForState) {
          if (log.isDebugEnabled()) {
            log.debug("replica={} state={} waitForState={} isLive={}", replica, replica.getState(), finalWaitForState,
                coreContainer.getZkController().getZkStateReader().isNodeLive(replica.getNodeName()));
          }
          return true;
        }
        log.info("replica={} state={}", cname, replica.getState());
        return false;
      });

    } catch (TimeoutException | InterruptedException e) {
      it.rsp.add("msg", "Timeout in preprecovery for " + cname );
      it.rsp.add("success", "false");
      return;
    }

    it.rsp.add(RESPONSE_STATUS, "0");
  }

  public static class NotValidLeader extends SolrException {

    public NotValidLeader(String s) {
      super(ErrorCode.BAD_REQUEST, s);
    }
  }
}

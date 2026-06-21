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

import org.apache.solr.client.solrj.cloud.ShardTerms;
import org.apache.solr.common.ParWork;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;

/**
 * Start recovery of a core if its term is less than leader's term
 */
public class RecoveringCoreTermWatcher extends ZkShardTerms.CoreTermWatcher implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CoreDescriptor coreDescriptor;
  private final CoreContainer coreContainer;
  // used to prevent the case when term of other replicas get changed, we redo recovery
  // the idea here is with a specific term of a replica, we only do recovery one
  private volatile boolean closed;

  // we pass in the lastTermDoRecovery because we have just registered and will be recovering from leader and want
  // this value to match up
  RecoveringCoreTermWatcher(CoreDescriptor coreDescriptor, CoreContainer coreContainer) {
    this.coreDescriptor = coreDescriptor;
    this.coreContainer = coreContainer;
  }

  @Override
  public boolean onTermChanged(ShardTerms terms) {
    if (coreContainer.isShutDown()) return false;
    MDCLoggingContext.setCoreName(coreDescriptor.getName());
    try {
      if (closed) {
        return false;
      }
      String coreName = coreDescriptor.getName();
      if (terms.haveHighestTermValue(coreName)) return true;

      log.info("Start recovery on {} because core's term is less than leader's term", coreName);
      // Do NOT call leaderElector.retryElection(...) here. A term-change watcher fires on EVERY
      // replica of the shard, including the current leader, and retryElection closes the elector
      // (cancelElection deletes this replica's election node, then it rejoins at the back). When a
      // recovering follower bumps the shard terms, the LEADER's watcher transiently sees itself as
      // not-highest and would tear down its own (head) election node -- the follower that is watching
      // it then fires NodeDeleted, reaches the head, and STEALS leadership. That hands leadership to a
      // just-restarted/behind replica, splits the slice, and silently diverges data: an update acked
      // with rf>=2 lands on the old leader while the freshly-promoted leader never has it
      // (TestTlogReplica.testRecovery: "Can not find doc N", ~40% under full-class contention). Upstream
      // Solr's RecoveringCoreTermWatcher only triggers recovery here; it never retries the election. The
      // election step-aside for a behind replica that happens to sit at the head is already handled
      // inside ShardLeaderElectionContext.runLeaderProcess (term-eligibility gate) and
      // LeaderElector.checkIfIamLeader (rejoinAtBack). Recovery is the correct, sufficient response to a
      // term-behind condition.
      try (SolrCore solrCore = coreContainer.getCore(coreDescriptor.getName())) {
        solrCore.getUpdateHandler().getSolrCoreState().doRecovery(solrCore.getCoreContainer(), solrCore.getCoreDescriptor(), "CoreTerm", null);
      }
      // Do not setTermEqualsToLeader here — doRecovery is async and has only been REQUESTED;
      // bumping the term now would mark a behind replica as up-to-date before it caught up.

    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      if (log.isInfoEnabled()) {
        log.info("Failed to watch term of core={}", coreDescriptor.getName(), e);
      }
      return false;
    } finally {
      MDCLoggingContext.clear();
    }

    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RecoveringCoreTermWatcher that = (RecoveringCoreTermWatcher) o;

    return coreDescriptor.getName().equals(that.coreDescriptor.getName());
  }

  @Override
  public int hashCode() {
    return coreDescriptor.getName().hashCode();
  }

  @Override
  public void close() {
   this.closed = true;
  }
}

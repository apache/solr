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

import com.codahale.metrics.Timer;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.stats.MetricUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command returns stats about the Overseer, the cluster state updater and collection API
 * activity occurring <b>within the current Overseer node</b> (this is important because distributed
 * operations occurring on other nodes are <b>not included</b> in these stats, for example
 * distributed cluster state updates or Per Replica States updates).
 *
 * <p>More fundamentally, when the Collection API command execution is distributed, this specific
 * command is <b>not</b> being run on the Overseer anyway (but then not much is running on the
 * overseer as cluster state updates are distributed as well) so Overseer stats and status can't be
 * returned and actually do not even make sense. Zookeeper based queue metrics do not make sense
 * either because Zookeeper queues are then not used.
 *
 * <p>The {@link Stats} instance returned by {@link CollectionCommandContext#getOverseerStats()}
 * when running in the Overseer is created in Overseer.start() and passed to the cluster state
 * updater from where it is also propagated to the various Zookeeper queues to register various
 * events. This class is the only place where it is used in the Collection API implementation, and
 * only to return results.
 *
 * <p>TODO: create a new command returning node specific Collection API/Config set API/cluster state
 * updates stats such as success and failures?
 *
 * <p>The structure of the returned results is as follows:
 *
 * <ul>
 *   <li><b>{@code leader}:</b> {@code ID} of the current overseer leader node
 *   <li><b>{@code overseer_queue_size}:</b> count of entries in the {@code /overseer/queue}
 *       Zookeeper queue/directory
 *   <li><b>{@code overseer_work_queue_size}:</b> count of entries in the {@code
 *       /overseer/queue-work} Zookeeper queue/directory
 *   <li><b>{@code overseer_collection_queue_size}:</b> count of entries in the {@code
 *       /overseer/collection-queue-work} Zookeeper queue/directory
 *   <li><b>{@code overseer_operations}:</b> map (of maps) of success and error counts for
 *       operations. The operations (keys) tracked in this map are:
 *       <ul>
 *         <li>{@code am_i_leader} (Overseer checking it is still the elected Overseer as it
 *             processes cluster state update messages)
 *         <li>{@code configset_}<i>{@code <config set operation>}</i>
 *         <li>Cluster state change operation names from {@link
 *             org.apache.solr.common.params.CollectionParams.CollectionAction} (not all of them!)
 *             and {@link org.apache.solr.cloud.overseer.OverseerAction} (the complete list: {@code
 *             create}, {@code delete}, {@code createshard}, {@code deleteshard}, {@code
 *             addreplica}, {@code addreplicaprop}, {@code deletereplicaprop}, {@code
 *             balanceshardunique}, {@code modifycollection}, {@code state}, {@code leader}, {@code
 *             deletecore}, {@code addroutingrule}, {@code removeroutingrule}, {@code
 *             updateshardstate}, {@code downnode} and {@code quit} with this last one unlikely to
 *             be observed since the Overseer is exiting right away)
 *         <li>{@code update_state} (when Overseer cluster state updater persists changes in
 *             Zookeeper)
 *       </ul>
 *       For each key, the value is a map composed of:
 *       <ul>
 *         <li>{@code requests}: success count of the given operation
 *         <li>{@code errors}: error count of the operation
 *         <li>More metrics (see below)
 *       </ul>
 *   <li><b>{@code collection_operations}:</b> map (of maps) of success and error counts for
 *       collection related operations. The operations(keys) tracked in this map are <b>all
 *       operations</b> that start with {@code collection_}, but the {@code collection_} prefix is
 *       <b>stripped</b> of the returned value. Possible keys are therefore:
 *       <ul>
 *         <li>{@code am_i_leader}: originating in a stat called {@code collection_am_i_leader}
 *             representing Overseer checking it is still the elected Overseer as it processes
 *             Collection API and Config Set API messages.
 *         <li>Collection API operation names from {@link
 *             org.apache.solr.common.params.CollectionParams.CollectionAction} (the stripped {@code
 *             collection_} prefix gets added in {@link
 *             OverseerCollectionMessageHandler#getTimerName(String)})
 *       </ul>
 *       For each key, the value is a map composed of:
 *       <ul>
 *         <li>{@code requests}: success count of the given operation
 *         <li>{@code errors}: error count of the operation
 *         <li>{@code recent_failures}: an <b>optional</b> entry containing a list of maps, each map
 *             having two entries, one with key {@code request} with a failed request properties (a
 *             {@link ZkNodeProps}) and the other with key {@code response} with the corresponding
 *             response properties (a {@link org.apache.solr.client.solrj.SolrResponse}).
 *         <li>More metrics (see below)
 *       </ul>
 *   <li><b>{@code overseer_queue}:</b> metrics on operations done on the Zookeeper queue {@code
 *       /overseer/queue} (see metrics below).<br>
 *       The operations that can be done on the queue and that can be keys whose values are a
 *       metrics map are:
 *       <ul>
 *         <li>{@code offer}
 *         <li>{@code peek}
 *         <li>{@code peek_wait}
 *         <li>{@code peek_wait_forever}
 *         <li>{@code peekTopN_wait}
 *         <li>{@code peekTopN_wait_forever}
 *         <li>{@code poll}
 *         <li>{@code remove}
 *         <li>{@code remove_event}
 *         <li>{@code take}
 *       </ul>
 *   <li><b>{@code overseer_internal_queue}:</b> same as above but for queue {@code
 *       /overseer/queue-work}
 *   <li><b>{@code collection_queue}:</b> same as above but for queue {@code
 *       /overseer/collection-queue-work}
 * </ul>
 *
 * <p>Maps returned as values of keys in <b>{@code overseer_operations}</b>, <b>{@code
 * collection_operations}</b>, <b>{@code overseer_queue}</b>, <b>{@code overseer_internal_queue}</b>
 * and <b>{@code collection_queue}</b> include additional stats. These stats are provided by {@link
 * MetricUtils}, and represent metrics on each type of operation execution (be it failed or
 * successful), see calls to {@link Stats#time(String)}. The metric keys are:
 *
 * <ul>
 *   <li>{@code avgRequestsPerSecond}
 *   <li>{@code 5minRateRequestsPerSecond}
 *   <li>{@code 15minRateRequestsPerSecond}
 *   <li>{@code avgTimePerRequest}
 *   <li>{@code medianRequestTime}
 *   <li>{@code 75thPcRequestTime}
 *   <li>{@code 95thPcRequestTime}
 *   <li>{@code 99thPcRequestTime}
 *   <li>{@code 999thPcRequestTime}
 * </ul>
 */
public class OverseerStatusCmd implements CollApiCmds.CollectionApiCommand {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CollectionCommandContext ccc;

  public OverseerStatusCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    // If Collection API execution is distributed, we're not running on the Overseer node so can't
    // return any Overseer stats.
    if (ccc.getCoreContainer().getDistributedCollectionCommandRunner().isPresent()) {
      // TODO: introduce a per node status command allowing insight into how Cluster state updates,
      // Collection API and config set API execution went on that node...
      return;
    }

    ZkStateReader zkStateReader = ccc.getZkStateReader();
    String leaderNode = OverseerTaskProcessor.getLeaderNode(zkStateReader.getZkClient());
    results.add("leader", leaderNode);
    Stat stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue", null, stat, true);
    results.add("overseer_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue-work", null, stat, true);
    results.add("overseer_work_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/collection-queue-work", null, stat, true);
    results.add("overseer_collection_queue_size", stat.getNumChildren());

    NamedList<Object> overseerStats = new NamedList<>();
    NamedList<Object> collectionStats = new NamedList<>();
    NamedList<Object> stateUpdateQueueStats = new NamedList<>();
    NamedList<Object> workQueueStats = new NamedList<>();
    NamedList<Object> collectionQueueStats = new NamedList<>();
    Stats stats = ccc.getOverseerStats();
    for (Map.Entry<String, Stats.Stat> entry : stats.getStats().entrySet()) {
      String key = entry.getKey();
      NamedList<Object> lst = new SimpleOrderedMap<>();
      if (key.startsWith("collection_")) {
        collectionStats.add(key.substring(11), lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
        List<Stats.FailedOp> failureDetails = stats.getFailureDetails(key);
        if (failureDetails != null) {
          List<SimpleOrderedMap<Object>> failures = new ArrayList<>();
          for (Stats.FailedOp failedOp : failureDetails) {
            SimpleOrderedMap<Object> fail = new SimpleOrderedMap<>();
            fail.add("request", failedOp.req.getProperties());
            fail.add("response", failedOp.resp.getResponse());
            failures.add(fail);
          }
          lst.add("recent_failures", failures);
        }
      } else if (key.startsWith("/overseer/queue_")) {
        stateUpdateQueueStats.add(key.substring(16), lst);
      } else if (key.startsWith("/overseer/queue-work_")) {
        workQueueStats.add(key.substring(21), lst);
      } else if (key.startsWith("/overseer/collection-queue-work_")) {
        collectionQueueStats.add(key.substring(32), lst);
      } else {
        // overseer stats
        overseerStats.add(key, lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
      }
      Timer timer = entry.getValue().requestTime;
      MetricUtils.addMetrics(lst, timer);
    }

    results.add("overseer_operations", overseerStats);
    results.add("collection_operations", collectionStats);
    results.add("overseer_queue", stateUpdateQueueStats);
    results.add("overseer_internal_queue", workQueueStats);
    results.add("collection_queue", collectionQueueStats);
  }
}

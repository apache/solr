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

import com.codahale.metrics.Meter;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

public class StatePublisher implements Closeable {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  static Meter cacheHits = Metrics.MARKS_METRICS.meter("statepublisher_cache_hits");
  static Meter published = Metrics.MARKS_METRICS.meter("statepublisher_published");


  public static final String OPERATION = "op";
  private volatile boolean closed;
  private volatile ExecutorService workerExec;

  private static class CacheEntry {
    Replica.State state;
    long time;
  }

  private final Map<String,CacheEntry> stateCache = new NonBlockingHashMap<>(16);
  private final ZkStateReader zkStateReader;
  private final CoreContainer cc;

  /**
   * Bounded duplicate suppression. Bounded by BOTH size (LRU eviction) and age (stale entries
   * ignored), so the map can never grow unbounded the way the old commented-out {@code stateCache}
   * dedup could.
   */
  private static final int DEDUP_MAX_SIZE = Integer.getInteger("solr.statePublisher.dedupMaxSize", 20000);
  private static final long DEDUP_MAX_AGE_MS = Long.getLong("solr.statePublisher.dedupMaxAgeMs", 30000L);

  /** id -> (shortState, recordedAtMs). Size-bounded LRU; access under its own monitor. */
  private final LinkedHashMap<String,long[]> dedupCache =
      new LinkedHashMap<String,long[]>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String,long[]> eldest) {
          return size() > DEDUP_MAX_SIZE;
        }
      };

  /**
   * True iff {@code id} was already published at {@code shortState} within the age window. Records the
   * (id, shortState, now) on a miss. Bounded by size (LRU) and age. {@code now} is supplied by the
   * caller to keep this testable.
   */
  private boolean isDuplicatePublish(String id, int shortState, long now) {
    synchronized (dedupCache) {
      long[] prev = dedupCache.get(id);
      if (prev != null && prev[0] == shortState && (now - prev[1]) <= DEDUP_MAX_AGE_MS) {
        return true;
      }
      dedupCache.put(id, new long[] {shortState, now});
      return false;
    }
  }

  public static class NoOpMessage extends ZkNodeProps {
  }
  static final String PREFIX = "qn-";
  public static final NoOpMessage TERMINATE_OP = new NoOpMessage();
  public static final Map TERMINATE_OP_MAP = new HashMap(0);

  private final LinkedTransferQueue<Map> workQueue = new LinkedTransferQueue<>();

  private volatile Worker worker;

  private volatile boolean terminated;
  private class Worker implements Runnable {

    public static final int POLL_TIME_ON_PUBLISH_NODE = 1;
    public static final int POLL_TIME = 250;

    Worker() {

    }

    @Override
    public void run() {

      while (!terminated) {
        Map message = null;
        Map bulkMessage = new HashMap();
        bulkMessage.put(OPERATION, "state");
        int pollTime = 250;
        try {
          try {
            log.debug("State publisher will poll for 5 seconds");
            message = workQueue.poll(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            message = TERMINATE_OP_MAP;
            terminated = true;
          } catch (Exception e) {
            log.warn("state publisher hit exception polling", e);
          }
          if (message != null) {
            log.debug("Got state message {}", message);

            if (message == TERMINATE_OP_MAP) {
              log.debug("State publish is terminated");
              message = TERMINATE_OP_MAP;
              terminated = true;
              pollTime = 1;
            } else {
              pollTime = bulkMessage(message, bulkMessage);
            }

            while (true) {
              try {
                log.debug("State publisher will poll for {} ms", pollTime);
                message = workQueue.poll(pollTime, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                message = TERMINATE_OP_MAP;
                terminated = true;
              } catch (Exception e) {
                log.warn("state publisher hit exception polling", e);
              }
              if (message != null) {
                if (log.isDebugEnabled()) log.debug("Got state message {}", message);
                if (message == TERMINATE_OP_MAP) {
                  terminated = true;
                  pollTime = 1;
                } else {
                  pollTime = bulkMessage(message, bulkMessage);
                }
              } else {
                break;
              }
            }
          }

          if (bulkMessage.size() > 1) {
            processMessage(bulkMessage);
          } else {
            log.debug("No messages to publish, loop");
          }

          if (terminated) {
            log.info("State publisher has terminated");
            break;
          }
        } catch (KeeperException.ConnectionLossException e) {
          log.warn("connection loss to zk", e);
          zkStateReader.getZkClient().getConnectionManager().waitForConnected();
        } catch (Exception e) {
          log.error("Exception in StatePublisher run loop", e);
        }
      }
    }

    private int bulkMessage(Map zkNodeProps, Map bulkMessage) {
      if (zkNodeProps.equals(TERMINATE_OP_MAP)) {
        return 0;
      }

      if (OverseerAction.get((String) zkNodeProps.get(OPERATION)) == OverseerAction.DOWNNODE) {
        String nodeName = (String) zkNodeProps.get(ZkStateReader.NODE_NAME_PROP);
        //clearStatesForNode(bulkMessage, nodeName);
        bulkMessage.put(OverseerAction.DOWNNODE.toLower(), nodeName);
        log.debug("add state to batch  down node, props={} result={}", zkNodeProps, bulkMessage);
        return 1;
      } else if (OverseerAction.get((String) zkNodeProps.get(OPERATION)) == OverseerAction.RECOVERYNODE) {
        log.debug("add state to batch  recovery node, props={} result={}", zkNodeProps, bulkMessage);
        String nodeName = (String) zkNodeProps.get(ZkStateReader.NODE_NAME_PROP);
       // clearStatesForNode(bulkMessage, nodeName);
        bulkMessage.put(OverseerAction.RECOVERYNODE.toLower(), nodeName);
        log.debug("add state to batch  recovery node, props={} result={}" , zkNodeProps, bulkMessage);
        return 1;
      } else {
        //String collection = zkNodeProps.getStr(ZkStateReader.COLLECTION_PROP);
        String core = (String) zkNodeProps.get(ZkStateReader.CORE_NAME_PROP);
        String id = (String) zkNodeProps.get("id");
        Replica.State state = (Replica.State) zkNodeProps.get(ZkStateReader.STATE_PROP);

        if (state == null) {
          log.error("Found null state in state update message={}", zkNodeProps);
          return 50;
        }

        Integer line = Replica.State.getShortState(state);
        if (log.isDebugEnabled()) log.debug("add state to batch core={} id={} state={} line={}", core, id, state, line);
        bulkMessage.put(id, line);
        if (state == Replica.State.LEADER) {
          return 1;
        } else if (state == Replica.State.ACTIVE) {
          return 25;
        } else {
          return 50;
        }
      }

    }

//    private void clearStatesForNode(ZkNodeProps bulkMessage, String nodeName) {
//      Set<String> removeIds = new HashSet<>();
//      Set<String> ids = bulkMessage.getProperties().keySet();
//      for (String id : ids) {
//        if (id.equals(OverseerAction.DOWNNODE.toLower()) || id.equals(OverseerAction.RECOVERYNODE.toLower())) {
//          continue;
//        }
//        Collection<DocCollection> collections = zkStateReader.getClusterState().getCollectionsMap().values();
//        for (DocCollection collection : collections) {
//          Replica replica = collection.getReplicaById(id);
//          if (replica != null) {
//            if (replica.getNodeName().equals(nodeName)) {
//              removeIds.add(id);)o(
//            }
//          }
//        }
//
//      }
//      for (String id : removeIds) {
//        bulkMessage.getProperties().remove(id);
//      }
//    }

    private void processMessage(Map message) throws KeeperException, InterruptedException {
      log.debug("Send state updates to Overseer {}", message);
      byte[] updates = Utils.toJSON(message);
      try {
        zkStateReader.getZkClient().create("/overseer/queue" + '/' + PREFIX, updates, CreateMode.PERSISTENT_SEQUENTIAL, (rc, path, ctx, name, stat) -> {
          if (rc != 0) {

            KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path);
            log.error("Exception publish state messages path={}", path, e);

            // This bulk message was NOT persisted. Re-offering the already-batched map is unsafe (it has
            // op=state with id->line entries the worker's bulkMessage() can't re-interpret), so instead we
            // drop the dedup records: otherwise the 30s dedup window would suppress the caller's identical
            // retry and the failed (non-LEADER) transition would be lost permanently. Clearing is bounded
            // and harmless — any healthy id simply gets to republish once (idempotent).
            synchronized (dedupCache) {
              dedupCache.clear();
            }
          }
        });
      } catch (KeeperException.ConnectionLossException e) {
        log.error("Exception publish state messages (synchronous ConnectionLoss)", e);
        // The bulk message was NOT persisted and the async rc-callback never fired for it, so the dedup
        // records stamped at submitState() time are stale. Drop them — identical to the rc!=0 branch
        // above — so the 30s dedup window cannot suppress the caller's identical retry of the failed
        // (non-LEADER) transition; otherwise that transition is lost permanently. Clearing is bounded
        // and idempotent (any healthy id simply republishes once).
        synchronized (dedupCache) {
          dedupCache.clear();
        }
      }
    }
  }

  public StatePublisher(ZkStateReader zkStateReader, CoreContainer cc) {
    this.zkStateReader = zkStateReader;
    this.cc = cc;
  }

  public void submitState(ZkNodeProps stateMessage) {
    // Don't allow publish of state we last published if not DOWNNODE?
    // Reject submits after close(): the worker has consumed its TERMINATE pill and exited, so anything
    // enqueued now is silently dropped (and workerExec may be null if start() never ran). A late
    // registration finishing during shutdown must not pretend to publish onto a dead worker.
    if (closed) {
      log.warn("Skipping state publish; StatePublisher is closed message={}", stateMessage);
      return;
    }
    try {
      if (stateMessage != TERMINATE_OP) {
        published.mark();
        String operation = stateMessage.getStr(OPERATION);
        String id = null;
        if (operation.equals("state")) {
          String core = stateMessage.getStr(ZkStateReader.CORE_NAME_PROP);
          String collection = stateMessage.getStr(ZkStateReader.COLLECTION_PROP);
          Replica.State state = (Replica.State) stateMessage.get(ZkStateReader.STATE_PROP);

          log.debug("submit state for publishing core={} state={}", core, state);

          if (core == null || state == null) {
            log.error("Nulls in published state");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Nulls in published state " + stateMessage);
          }

          DocCollection coll = zkStateReader.getCollectionOrNull(collection);

          if (coll != null) {
            Replica replica = coll.getReplica(core);
            if (replica != null) {
              id = replica.getId();
            } else {
              id = stateMessage.getStr("id");
            }

          }

          if (id == null) {
            id = stateMessage.getStr("id");
          } else {
            stateMessage.getProperties().putIfAbsent("id", id);
          }

          // Bounded dedup. LEADER is NEVER dedup-suppressed: a LEADER publish is the repair mechanism for the single
          // most important state in the channel. On failover the overseer's in-memory stateUpdates map
          // can lose the leader entry (new overseer, seed miss), and the only repair is a LEADER
          // republish (ShardLeaderElectionContext.runLeaderProcess, RecoveryStrategy "we are the leader,
          // STOP recovery"). Suppressing a repeat LEADER within the 30s age window left getLeader()
          // returning empty for the shard, wedging recovery (TestTlogReplica). LEADER is already treated
          // as flush-immediately (bulkMessage returns pollTime=1), so it must always reach the plane.
          if (id != null && state != Replica.State.LEADER) {
            int shortState = Replica.State.getShortState(state);
            if (isDuplicatePublish(id, shortState, System.currentTimeMillis())) {
              cacheHits.mark();
              if (log.isDebugEnabled()) {
                log.debug("Skip duplicate publish state={} for core={} id={} (bounded dedup)", state, core, id);
              }
              return;
            }
          }

//          CacheEntry lastState = stateCache.get(id);
//          if (lastState != null && state.equals(lastState.state)) {
//            cacheHits.mark();
//            log.info("Skipping publish state as {} for {}, because it was the last state published", state, core);
//            return;
//          }
//
//          CacheEntry cacheEntry = new CacheEntry();
//          cacheEntry.time = System.currentTimeMillis();
//          cacheEntry.state = state;
//          stateCache.put(id, cacheEntry);

          //        else if (operation.equalsIgnoreCase(OverseerAction.DOWNNODE.toLower())) {
          //          // set all statecache entries for replica to a state
          //
          //          Collection<CoreDescriptor> cds = cc.getCoreDescriptors();
          //          for (CoreDescriptor cd : cds) {
          //            DocCollection doc = zkStateReader.getCollectionOrNull(cd.getCollectionName());
          //            Replica replica = null;
          //            if (doc != null) {
          //              replica = doc.getReplica(cd.getName());
          //
          //              if (replica != null) {
          //                CacheEntry cacheEntry = new CacheEntry();
          //                cacheEntry.time = System.currentTimeMillis();
          //                cacheEntry.state = Replica.State.getShortState(Replica.State.DOWN);
          //                stateCache.put(replica.getId(), cacheEntry);
          //              }
          //            }
          //          }
          //
          //        } else if (operation.equalsIgnoreCase(OverseerAction.RECOVERYNODE.toLower())) {
          //          // set all statecache entries for replica to a state
          //
          //          Collection<CoreDescriptor> cds = cc.getCoreDescriptors();
          //          for (CoreDescriptor cd : cds) {
          //            DocCollection doc = zkStateReader.getCollectionOrNull(cd.getCollectionName());
          //            Replica replica = null;
          //            if (doc != null) {
          //              replica = doc.getReplica(cd.getName());
          //
          //              if (replica != null) {
          //                CacheEntry cacheEntry = new CacheEntry();
          //                cacheEntry.time = System.currentTimeMillis();
          //                cacheEntry.state = Replica.State.getShortState(Replica.State.RECOVERING);
          //                stateCache.put(replica.getId(), cacheEntry);
          //              }
          //            }
        }
      }
      //      else {
      //        log.error("illegal state message {}", stateMessage.toString());
      //        throw new IllegalArgumentException(stateMessage.toString());
      //      }
      //

      if (stateMessage == TERMINATE_OP) {
        //if (!workQueue.tryTransfer(TERMINATE_OP_MAP)) {
          workQueue.put(TERMINATE_OP_MAP);
     //   }
      } else {
        Map<String,Object> props = stateMessage.getProperties();
      //  if (!workQueue.tryTransfer(props)) {
          workQueue.put(props);
      //  }
      }
    } catch (Exception e) {
      log.error("Exception trying to publish state message={}", stateMessage, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }

    public void clearStatCache (String core){
      stateCache.remove(core);
    }

    public void clearStatCache () {
      stateCache.clear();
      synchronized (dedupCache) {
        dedupCache.clear();
      }
    }

    public void start () {
      this.worker = new Worker();
      workerExec = Executors.newSingleThreadExecutor(new SolrNamedThreadFactory("StatePublisher", true));

      workerExec.submit(this.worker);
    }

    public void close () {
      // Set closed FIRST so submitState() stops accepting work before we stop the worker; otherwise a
      // concurrent publish could enqueue after the TERMINATE pill and be lost. Idempotent: close() is
      // invoked twice from ZkController (explicit close() + closeQuietly).
      if (closed) {
        return;
      }
      this.closed = true;

      workQueue.put(TERMINATE_OP_MAP);

      // workerExec is null if start() was never called (e.g. a ZkController that failed mid-init).
      ExecutorService exec = this.workerExec;
      if (exec != null) {
        exec.shutdown();
        try {
          exec.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // Restore the interrupt flag rather than swallowing it, so callers up the close() chain see it.
          Thread.currentThread().interrupt();
        }
      }
    }
  }

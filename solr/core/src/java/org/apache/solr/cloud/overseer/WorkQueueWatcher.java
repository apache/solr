package org.apache.solr.cloud.overseer;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.StatePublisher;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class WorkQueueWatcher extends QueueWatcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Overseer overseer;

  private volatile boolean checkAgain = false;
  private volatile boolean running;

  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Upper bound on how long an item's processing waits for its state-plane append to become durable
   * before the item is left in the queue for idempotent reprocess (finding #5). The publish itself is
   * bounded by ZK session/retry limits, so this is a backstop, not the normal completion path.
   */
  private static final long DURABLE_APPEND_TIMEOUT_MS =
      Long.getLong("solr.overseer.queueItemDurabilityTimeoutMs", 30000L);

  public WorkQueueWatcher(Overseer overseer, CoreContainer cc) throws KeeperException {
    super(cc, overseer, Overseer.OVERSEER_QUEUE);
    this.overseer = overseer;
  }

  public void start(boolean weAreReplacement) throws KeeperException, InterruptedException {
    if (closed) return;
    zkController.getZkClient().addWatch(path, this, AddWatchMode.PERSISTENT);
    Queue<String> startItems = getItems(true);
    log.info("Overseer found entries on start {} {}", startItems, path);
    if (startItems.size() > 0) {
      processQueueItems(startItems, true, weAreReplacement);
    }

  }

  protected Queue<String> getItems(boolean onStart) {
    try {
      List<String> children = zkController.getZkClient().getChildren(path, null, null, true, false);
      if (log.isDebugEnabled()) {
        log.debug("get items from Overseer state work queue onStart={} {} {}", onStart, path, children.size());
      }
      List<String> items = new ArrayList<>(children);
      Collections.sort(items);
      log.debug("sorted state items from zk queue={}", items);
      return new LinkedTransferQueue<>(items);
    } catch (KeeperException.SessionExpiredException e) {
      log.warn("ZooKeeper session expired");
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (AlreadyClosedException e) {
      throw e;
    } catch (Exception e) {
      log.error("Unexpected error in Overseer state update loop", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public void processEvent(WatchedEvent event) {
    if (!Event.EventType.NodeChildrenChanged.equals(event.getType())) {
      return;
    }
    if (this.closed || zkController.getZkClient().isClosed()) {
      log.info("Overseer is closed, do not process watcher for queue");
      return;
    }

    lock.lock();
    try {

      if (running) {
        checkAgain = true;
      } else {
        running = true;
        overseer.getTaskZkWriterExecutor().submit(() -> {
          try {
            do {
              checkAgain = false;
              Queue items = getItems(false);
              try {

                if (items.size() > 0) {
                  processQueueItems(items, false, false);
                }
              } catch (AlreadyClosedException e) {

              } catch (Exception e) {
                log.error("Exception during overseer queue queue processing", e);
              }

              // Make the stop-decision under the same lock processEvent uses to set checkAgain / read
              // running. Unlocked, a checkAgain=true set by processEvent could land between our read here
              // and our running=false write, leaving checkAgain==true with running==false and nothing
              // scheduled — the last update (e.g. a final DOWNNODE with no following watch event) stalls.
              lock.lock();
              try {
                if (!checkAgain) {
                  running = false;
                  break;
                }
              } finally {
                lock.unlock();
              }

            } while (true);

          } catch (Exception e) {
            log.error("exception submitting queue task", e);
          }
        });
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected Set<Integer> processQueueItems(Queue<String> items, boolean onStart, boolean weAreReplacement) {
    if (items.size() == 0) {
      return Collections.emptySet();
    }
    List<String> fullPaths = new ArrayList<>(items.size());

    log.debug("Found state update queue items {}", items);

    for (String item : items) {
      fullPaths.add(path + '/' + item);
    }

    Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);
    Set<Integer> collIds = new HashSet<>();
    // Per-item durability gate (finding #5): an item is deleted only after the future for the
    // state-plane append that carries its update completes. Items applied as synchronous structure
    // changes (already durable) map to an already-completed future.
    final Map<String,CompletableFuture<Void>> durableByKey = new LinkedHashMap<>();
    data.forEach((key, value) -> {
      Map<Integer,Map<Integer,Integer>> replicaStates = new ConcurrentHashMap<>();
      Map<Integer,List<ZkStateWriter.StateUpdate>> sliceStates = new ConcurrentHashMap<>();
      final ZkNodeProps message = ZkNodeProps.load(value);

      log.debug("add state update {}", message);

      final String op = message.getStr(StatePublisher.OPERATION);
      message.getProperties().remove(StatePublisher.OPERATION);
      OverseerAction overseerAction = OverseerAction.get(op);
      if (overseerAction == null) {
        // Some collection commands (Reindex/Migrate/Restore/Split) enqueue collection structure-change
        // operations (e.g. MODIFYCOLLECTION) onto the state-update queue via Overseer.offerStateUpdate.
        // These carry a CollectionParams.CollectionAction in the "op" field, not an OverseerAction, so they
        // must be applied as structure changes here. Never let an unrecognized op throw: that would abort
        // the whole batch and (since the item is then not deleted) be reprocessed forever, permanently
        // poisoning the overseer state-update queue.
        applyCollectionStateUpdate(op, message);
        // Structure changes are applied synchronously via writePendingUpdates().get() — already durable.
        durableByKey.put(key, CompletableFuture.completedFuture(null));
      } else {
        switch (overseerAction) {
          case STATE:
            processStateUpdateNode(onStart, weAreReplacement, collIds, replicaStates, message);
            processStateUpdateReplica(onStart, weAreReplacement, collIds, replicaStates, message);
            break;
          case UPDATESHARDSTATE:
            try {
              int id = ((Long) message.getProperties().remove("id")).intValue();

              List<ZkStateWriter.StateUpdate> updates = sliceStates.computeIfAbsent(id, k -> new ArrayList<>());

              Set<Map.Entry<String,Object>> entries = message.getProperties().entrySet();
              for (Map.Entry<String,Object> entry : entries) {
                ZkStateWriter.StateUpdate update = new ZkStateWriter.StateUpdate();
                update.sliceName = entry.getKey();
                update.state = Slice.State.getState((String) entry.getValue());
                updates.add(update);
              }
            } catch (Exception e) {
              log.error("Overseer slice state update queue processing failed {}", message, e);
            }
            break;

          case ADDROUTINGRULE:
          case REMOVEROUTINGRULE:
            // MigrateCmd adds/removes a routing rule on the source slice (so updates for the migrated
            // route-key are forwarded to the target collection) by offering an ADDROUTINGRULE op onto this
            // state-update queue. The StateUpdates overseer otherwise only handles STATE/UPDATESHARDSTATE, so
            // without this the op was ignored and MigrateCmd timed out with "Could not add routing rule",
            // migrating 0 docs. Apply it as a structure change, same pattern as MODIFYCOLLECTION below.
            applyRoutingRuleStateUpdate(overseerAction, message);
            break;

          default:
            log.warn("Ignoring unsupported overseer state-update op={} contents={}", op, message);
            break;
        }
        // enqueueStateUpdates returns a future for any slice-state (UPDATESHARDSTATE) structure write it
        // scheduled. writeStateUpdates returns a future that completes once the delta-plane replica-state
        // append is durable. enqueueStateUpdates already recorded this item's change in the in-memory map,
        // so the append future this tracks necessarily carries it.
        CompletableFuture<Void> structureFuture =
            overseer.getZkStateWriter().enqueueStateUpdates(replicaStates, sliceStates);
        try {
          // Gate the queue-item delete on BOTH futures: a pure UPDATESHARDSTATE item adds nothing to
          // collIds, so its append future is trivially complete — without the structure future it could be
          // deleted before its slice hard-state write is durable. (finding #2)
          CompletableFuture<Void> appendFuture = overseer.getZkStateWriter().writeStateUpdates(collIds);
          durableByKey.put(key, CompletableFuture.allOf(structureFuture, appendFuture));
        } catch (InterruptedException e) {
          log.warn("interrupted", e);
          throw new AlreadyClosedException(e);
        }
      }
    });

    // Second pass: delete each queue item only after its update is durably appended to the state plane.
    // All units were enqueued above, so the ZkStateWriter Worker coalesces them into batched publishes;
    // we do not serialize one publish per item. On timeout/failure the item is left for reprocess (the
    // publish is idempotent — it re-reads effective state and suppresses no-ops).
    for (Map.Entry<String,CompletableFuture<Void>> e : durableByKey.entrySet()) {
      if (overseer.getTaskZkWriterExecutor().isShutdown()) {
        break;
      }
      final String key = e.getKey();
      try {
        e.getValue().get(DURABLE_APPEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new AlreadyClosedException(ie);
      } catch (TimeoutException te) {
        log.warn("state-plane append for queue item {} not durable within {}ms; leaving it for reprocess",
            key, DURABLE_APPEND_TIMEOUT_MS);
        continue;
      } catch (ExecutionException ee) {
        log.warn("state-plane append for queue item {} failed; leaving it for reprocess", key, ee);
        continue;
      }
      try {
        zkController.getZkClient().delete(key, -1);
      } catch (Exception ex) {
        log.warn("Failed deleting processed items", ex);
      }
    }

    return collIds;
  }

  /**
   * Apply a collection structure-change operation that was enqueued onto the state-update queue via
   * {@link Overseer#offerStateUpdate(byte[])} (e.g. MODIFYCOLLECTION from Reindex/Migrate/Restore/Split,
   * which set readOnly / reindexing-state collection properties). The state-update queue normally only
   * carries {@link OverseerAction} STATE/UPDATESHARDSTATE messages; these collection actions carry a
   * {@link CollectionParams.CollectionAction} instead and must be applied as a structure change here.
   */
  private void applyCollectionStateUpdate(String op, ZkNodeProps message) {
    CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(op);
    if (action == CollectionParams.CollectionAction.MODIFYCOLLECTION) {
      String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
      try {
        ClusterState cs = overseer.getZkStateWriter().getClusterstate(collection);
        if (cs == null || cs.getCollectionOrNull(collection) == null) {
          log.warn("MODIFYCOLLECTION state update for unknown collection {}: {}", collection, message);
          return;
        }
        cs = CollectionMutator.modifyCollection(cs, message);
        DocCollection docColl = cs.getCollectionOrNull(collection);
        if (docColl != null) {
          overseer.getZkStateWriter().enqueueStructureChange(docColl);
          Future f = overseer.writePendingUpdates(collection);
          if (f != null) {
            f.get();
          }
        }
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new AlreadyClosedException(e);
      } catch (Exception e) {
        log.error("Failed applying MODIFYCOLLECTION state update for {}: {}", collection, message, e);
      }
    } else {
      log.warn("Ignoring unsupported overseer state-update op={} contents={}", op, message);
    }
  }

  /**
   * Apply an ADDROUTINGRULE / REMOVEROUTINGRULE op (offered by MigrateCmd via
   * {@link Overseer#offerStateUpdate(byte[])}) as a collection structure change, using
   * {@link SliceMutator}. The state-update queue normally only carries STATE/UPDATESHARDSTATE; routing-rule
   * ops carry an {@link OverseerAction} and must be turned into a slice structure change here so the rule is
   * persisted to state.json and seen by readers (otherwise MigrateCmd times out waiting for the rule).
   */
  private void applyRoutingRuleStateUpdate(OverseerAction action, ZkNodeProps message) {
    String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
    try {
      ClusterState cs = overseer.getZkStateWriter().getClusterstate(collection);
      if (cs == null || cs.getCollectionOrNull(collection) == null) {
        log.warn("{} state update for unknown collection {}: {}", action, collection, message);
        return;
      }
      ClusterState updated = (action == OverseerAction.ADDROUTINGRULE)
          ? SliceMutator.addRoutingRule(cs, message)
          : SliceMutator.removeRoutingRule(cs, message);
      if (updated == null) {
        return;
      }
      DocCollection docColl = updated.getCollectionOrNull(collection);
      if (docColl != null) {
        overseer.getZkStateWriter().enqueueStructureChange(docColl);
        Future f = overseer.writePendingUpdates(collection);
        if (f != null) {
          f.get();
        }
      }
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new AlreadyClosedException(e);
    } catch (Exception e) {
      log.error("Failed applying {} state update for {}: {}", action, collection, message, e);
    }
  }

  private void processStateUpdateNode(boolean onStart, boolean weAreReplacement, Set<Integer> collIds, Map<Integer,Map<Integer,Integer>> replicaStates,
      ZkNodeProps message) {
    for (Map.Entry<String,Object> theEntry : message.getProperties().entrySet()) {
      Map.Entry<String,Object> entry = theEntry;
      log.debug("process state update entry {}", entry);
      try {

        if (OverseerAction.RECOVERYNODE.toLower().equals(entry.getKey()) || OverseerAction.DOWNNODE.toLower().equals(entry.getKey())) {
          if (onStart && !weAreReplacement) {
            if (log.isDebugEnabled()) {
              log.debug("Got {}, but we are not a replacement leader, looks like a fresh start, ignoring ...", entry.getKey());
            }
            continue;
          }

          Integer state = null;
          if (OverseerAction.DOWNNODE.toLower().equals(entry.getKey())) {
            state = Replica.State.getShortState(Replica.State.DOWN);
            if (log.isDebugEnabled()) {
              log.debug("Process DOWNNODE for {} ... ", entry.getValue());
            }
          } else if (OverseerAction.RECOVERYNODE.toLower().equals(entry.getKey())) {
            if (log.isDebugEnabled()) {
              log.debug("Process RECOVERING for {} ... ", entry.getValue());
            }
            state = Replica.State.getShortState(Replica.State.RECOVERING);
          }
          String nodeName = (String) entry.getValue();

          // finding #6: resolve the node's replicas through the ZkStateWriter placement index, so this
          // scales O(replicas on this node) instead of scanning every collection and every replica. The
          // index key is the collection id (== Replica.getCollectionId() for replicas in that collection),
          // matching the prior scan's replicaStates key and collIds entry exactly.
          for (Map.Entry<Integer,Set<Integer>> ne : overseer.getZkStateWriter().getReplicasOnNode(nodeName).entrySet()) {
            Integer collId = ne.getKey();
            collIds.add(collId);
            Map<Integer,Integer> updates = replicaStates.computeIfAbsent(collId, k -> new ConcurrentHashMap<>());
            for (Integer internalId : ne.getValue()) {
              if (log.isDebugEnabled()) {
                log.debug("add state update id={} {} for collection {}", internalId, state, collId);
              }
              updates.remove(internalId);
              updates.put(internalId, state);
            }
          }
          continue;
        }
      } catch (Exception e) {
        log.error("Overseer state update queue processing failed entry-{}", theEntry, e);
      }

    }
  }

  private static void processStateUpdateReplica(boolean onStart, boolean weAreReplacement, Set<Integer> collIds,
      Map<Integer,Map<Integer,Integer>> replicaStates, ZkNodeProps message) {
    for (Map.Entry<String,Object> theEntry : message.getProperties().entrySet()) {
      Map.Entry<String,Object> entry = theEntry;
      log.debug("process state update entry {}", entry);
      try {

        if (OverseerAction.RECOVERYNODE.toLower().equals(entry.getKey()) || OverseerAction.DOWNNODE.toLower().equals(entry.getKey())) {
          continue;
        }
      } catch (Exception e) {
        log.error("Overseer state update queue processing failed entry-{}", theEntry, e);
      }

      try {
        String id = entry.getKey();
        Integer state = ((Long) entry.getValue()).intValue();
        int collId = Integer.parseInt(id.substring(0, id.indexOf('-')));

        Map<Integer,Integer> updates = replicaStates.computeIfAbsent(collId, k -> new ConcurrentHashMap<>());
        collIds.add(collId);
        Integer replicaId = Integer.parseInt(id.substring(id.indexOf('-') + 1));
        log.debug("add state update id={} {} for collection {}", replicaId, state, collId);
        updates.remove(replicaId);
        updates.put(replicaId, state);

      } catch (Exception e) {
        log.error("Overseer state update queue processing failed entry={}", theEntry, e);
      }
    }
  }

}

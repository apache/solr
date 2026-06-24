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

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.solr.client.solrj.cloud.ShardTerms;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for interact with a ZK term node.
 * Each ZK term node relates to a shard of a collection and have this format (in json)
 * <p>
 * <code>
 * {
 *   "replicaNodeName1" : 1,
 *   "replicaNodeName2" : 2,
 *   ..
 * }
 * </code>
 * <p>
 * The values correspond to replicas are called terms.
 * Only replicas with highest term value are considered up to date and be able to become leader and serve queries.
 * <p>
 * Terms can only updated in two strict ways:
 * <ul>
 * <li>A replica sets its term equals to leader's term
 * <li>The leader increase its term and some other replicas by 1
 * </ul>
 * This class should not be reused after {@link org.apache.zookeeper.Watcher.Event.KeeperState#Expired} event
 */
public class ZkShardTerms extends DoNotWrap implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String collection;
  private final String shard;
  private final String znodePath;
  private final SolrZkClient zkClient;

  private final Map<String, CoreTermWatcher> listeners = new ConcurrentHashMap<>();

  private final AtomicReference<ShardTerms> terms = new AtomicReference<>();
  // Guards the running/checkAgain coalescing gate in scheduleTermNotify. These flags are a
  // check-then-act pair; without a lock a producer's "checkAgain=true" can race the worker's
  // "running=false" exit and be stranded with no runner -> a term-change notification is lost
  // (E5-1 lost wakeup). All reads/writes of running+checkAgain now happen under this lock.
  private final ReentrantLock ourLock = new ReentrantLock();
  private boolean checkAgain = false;
  private boolean running;

  private volatile boolean closed;

  @Override
  public String toString() {
    return "ZkShardTerms{" + "terms=" + terms.get() + '}';
  }

  @Override
  public void processEvent(WatchedEvent event) {
    if (!Event.EventType.NodeDataChanged.equals(event.getType())) {
      return;
    }

    if (closed || zkClient.isClosed()) {
      return;
    }
    scheduleTermNotify(true);
  }

  // Serialized term-change notification. The CoreTermWatcher contract forbids concurrent
  // onTermChanged invocations, so all notifications (ZK-watch driven AND local saveTerms driven)
  // funnel through this single running/checkAgain gate. refreshFirst=true re-reads ZK first (used by
  // the ZK watch path); refreshFirst=false notifies against the already-updated in-memory terms
  // (used by setNewTerms after a local write, so a replica we just pushed to a lower term is told to
  // recover immediately instead of waiting for — or missing — the coalescable ZK watch round-trip).
  private void scheduleTermNotify(boolean refreshFirst) {
    if (closed || zkClient.isClosed()) {
      return;
    }
    // Claim the gate under the lock. If a worker is already running, record that another round is
    // needed and return; the running worker will observe checkAgain under the same lock before it
    // exits, so the request can never be stranded (E5-1).
    ourLock.lock();
    try {
      if (running) {
        checkAgain = true;
        return;
      }
      running = true;
    } finally {
      ourLock.unlock();
    }

    ParWork.getRootSharedExecutor().submit(() -> {
      try {
        while (true) {
          if (refreshFirst) refresh();
          onTermUpdates(this.terms.get());

          // Decide whether to loop again or exit, atomically with respect to producers setting
          // checkAgain. Either we see a pending request and re-run, or we clear running while
          // holding the lock so any later producer takes the !running branch and starts a fresh
          // worker. No notification can slip between the two.
          ourLock.lock();
          try {
            if (checkAgain) {
              checkAgain = false;
            } else {
              running = false;
              break;
            }
          } finally {
            ourLock.unlock();
          }
        }
      } catch (Exception e) {
        ourLock.lock();
        try {
          running = false;
        } finally {
          ourLock.unlock();
        }
        log.error("exception submitting queue task", e);
      }
    });
  }

  /**
   * Listener of a core for shard's term change events
   */
  abstract static class CoreTermWatcher implements Closeable {
    /**
     * Invoked with a Terms instance after update. <p>
     * Concurrent invocations of this method is not allowed so at a given time only one thread
     * will invoke this method.
     * <p>
     * <b>Note</b> - there is no guarantee that the terms version will be strictly monotonic i.e.
     * an invocation with a newer terms version <i>can</i> be followed by an invocation with an older
     * terms version. Implementations are required to be resilient to out-of-order invocations.
     *
     * @param terms instance
     * @return true if the listener wanna to be triggered in the next time
     */
    abstract boolean onTermChanged(ShardTerms terms);
  }

  public ZkShardTerms(String collection, String shard, SolrZkClient zkClient) {
    this.znodePath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms/" + shard;
    this.collection = collection;
    this.shard = shard;
    this.zkClient = zkClient;
    createWatcher();

    try {
      refresh();
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error refreshing ZkShardTerms", e);
    }

    assert ObjectReleaseTracker.getInstance().track(this);
  }

  private void createWatcher() {

    try {
      zkClient.addWatch(znodePath, this, AddWatchMode.PERSISTENT, true, true);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void removeWatcher() {
    try {
      zkClient.removeWatches(znodePath, this, WatcherType.Any, true);
    } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }
  /**
   * Ensure that leader's term is higher than some replica's terms
   * @param leader coreNodeName of leader
   * @param replicasNeedingRecovery set of replicas in which their terms should be lower than leader's term
   */
  public void ensureTermsIsHigher(String leader, Set<String> replicasNeedingRecovery) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) log.debug("ensureTermsIsHigher leader={} replicasNeedingRecvoery={}", leader, replicasNeedingRecovery);
    if (replicasNeedingRecovery.isEmpty()) return;
    ShardTerms newTerms;
    while( (newTerms = terms.get().increaseTerms(leader, replicasNeedingRecovery)) != null) {
      if (saveTerms(newTerms)) return;
    }
  }

  public ShardTerms getShardTerms() {
    return terms.get();
  }
  /**
   * Can this replica become leader?
   * @param coreNodeName of the replica
   * @return true if this replica can become leader, false if otherwise
   */
  public boolean canBecomeLeader(String coreNodeName) {
    return terms.get().canBecomeLeader(coreNodeName);
  }

  /**
   * Should leader skip sending updates to this replica?
   * @param coreNodeName of the replica
   * @return true if this replica has term equals to leader's term, false if otherwise
   */
  public boolean skipSendingUpdatesTo(String coreNodeName) {
    if (log.isDebugEnabled()) log.debug("check skipSendingUpdatesTo {} {}", coreNodeName, terms);

    return !terms.get().haveHighestTermValue(coreNodeName);
  }

  /**
   * Did this replica registered its term? This is a sign to check f
   * @param coreNodeName of the replica
   * @return true if this replica registered its term, false if otherwise
   */
  public boolean registered(String coreNodeName) {
    return terms.get().getTerm(coreNodeName) != null;
  }

  public void close() {
    // no watcher will be registered
    //isClosed.set(true);
    closed = true;
    listeners.values().forEach(coreTermWatcher -> IOUtils.closeQuietly(coreTermWatcher));
    listeners.clear();

    removeWatcher();

    assert ObjectReleaseTracker.getInstance().release(this);
  }

  // package private for testing, only used by tests
  Map<String, Long> getTerms() {
    return new HashMap<>(terms.get().getTerms());
  }

  /**
   * Add a listener so the next time the shard's term get updated, listeners will be called
   */
  void addListener(String core, CoreTermWatcher listener) {
    listeners.put(core, listener);
  }

  /**
   * Remove the coreNodeName from terms map and also remove any expired listeners
   * @return Return true if this object should not be reused
   */
  boolean removeTermFor(String name) throws KeeperException, InterruptedException {
    IOUtils.closeQuietly(listeners.remove(name));

    return removeTerm(name) || getNumListeners() == 0;
  }

  // package private for testing, only used by tests
  // return true if this object should not be reused
  boolean removeTerm(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    if (terms.get() == null) {
      return true;
    }
    while ( (newTerms = terms.get().removeTerm(coreNodeName)) != null) {
      try {
        if (saveTerms(newTerms)) {
          return false;
        }
      } catch (KeeperException.NoNodeException e) {
        return true;
      }
    }
    return true;
  }

  /**
   * Register a replica's term (term value will be 0).
   * If a term is already associate with this replica do nothing
   * @param coreNodeName of the replica
   * @return
   */
  public ShardTerms registerTerm(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ((newTerms = terms.get().registerTerm(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
    return newTerms;
  }

  /**
   * Set a replica's term equals to leader's term, and remove recovering flag of a replica.
   * This call should only be used by {@link org.apache.solr.common.params.CollectionParams.CollectionAction#FORCELEADER}
   * @param coreNodeName of the replica
   */
  public void setTermEqualsToLeader(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().setTermEqualsToLeader(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
  }

  public void setTermToZero(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().setTermToZero(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
  }

  /**
   * Mark {@code coreNodeName} as recovering
   */
  public void startRecovering(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().startRecovering(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
  }

  /**
   * Mark {@code coreNodeName} as finished recovering
   */
  public void doneRecovering(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().doneRecovering(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
  }

  public boolean isRecovering(String name) {
    return terms.get().isRecovering(name);
  }

  /**
   * When first updates come in, all replicas have some data now,
   * so we must switch from term 0 (registered) to 1 (have some data)
   */
  public void ensureHighestTermsAreNotZero() throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().ensureHighestTermsAreNotZero()) != null) {
      if (log.isDebugEnabled()) log.debug("Terms are at {}", terms.get());
      if (saveTerms(newTerms)) break;
    }
  }

  public long getHighestTerm() {
    return terms.get().getMaxTerm();
  }

  public long getTerm(String coreNodeName) {
    Long term = terms.get().getTerm(coreNodeName);
    return term == null? -1 : term;
  }

  // package private for testing, only used by tests
  int getNumListeners() {
    return listeners.size();
  }

  /**
   * Set new terms to ZK, the version of new terms must match the current ZK term node
   * @param newTerms to be set
   * @return true if terms is saved successfully to ZK, false if otherwise
   * @throws KeeperException.NoNodeException correspond ZK term node is not created
   */
  private boolean saveTerms(ShardTerms newTerms) throws KeeperException, InterruptedException {
    log.info("save terms {} {}", shard, newTerms);
    byte[] znodeData = Utils.toJSON(newTerms);

    try {
      Stat stat = zkClient.setData(znodePath, znodeData, newTerms.getVersion(), true);
      ShardTerms newShardTerms = new ShardTerms(newTerms, stat.getVersion());
      setNewTerms(newShardTerms);
      log.debug("Successful update of terms at {} to {}", znodePath, newTerms);
      return true;
    } catch (KeeperException.BadVersionException e) {
      int foundVersion = -1;
      Stat stat = zkClient.exists(znodePath, null);
      if (stat != null) {
        foundVersion = stat.getVersion();
      }
      log.info("Failed to save terms, version is not a match, retrying version={} found={}", newTerms.getVersion(), foundVersion);

      if (newTerms.getVersion() > foundVersion) {
        // Our in-memory version drifted AHEAD of ZK (term node deleted+recreated, or stale
        // in-memory ShardTerms). The CAS write did NOT happen. Returning true here used to report a
        // SUCCESSFUL save while writing nothing (E4-4) -> a silent term no-op: a replica could
        // publish ACTIVE with a stale ZK term (skipped by the leader's update fan-out) and leave its
        // _recovering marker stuck (can never become leader). We cannot simply refresh-and-retry
        // because setNewTerms refuses to pull the in-memory version back DOWN (version-monotonic
        // guard), which would livelock. Surface the failure to the caller instead of faking success.
        throw e;
      }

      refreshTerms(foundVersion);
    }
    return false;
  }

  /**
   * Fetch latest terms from ZK
   */
  public void refreshTerms(int version) throws KeeperException {

    ShardTerms newTerms;

    try {
      Stat stat = new Stat();
      byte[] data = zkClient.getData(znodePath, null, stat, true);
      if (data == null) {
        setNewTerms(new ShardTerms(Collections.emptyMap(), -1));
        return;
      }
      Map map = new Object2LongOpenHashMap<>((Map<String, Long>) Utils.fromJSON(data));
      Map<String,Long> values = Collections.unmodifiableMap(map);
      log.debug("refresh shard terms to zk version {} values={}", stat.getVersion(), values);
      newTerms = new ShardTerms(values, stat.getVersion());
    } catch (KeeperException.NoNodeException e) {
      log.info("No node found for shard terms {} znodepath={}", e.getPath(), znodePath);
      // we have likely been deleted
      setNewTerms(new ShardTerms(Collections.emptyMap(), -1));
      return;
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating shard term for collection: " + collection, e);
    }

    setNewTerms(newTerms);
  }

  /**
   * Retry register a watcher to the correspond ZK term node
   */
  public void refresh() throws KeeperException {
    refreshTerms(-1);
  }

  /**
   * Atomically update {@link ZkShardTerms#terms} and call listeners
   * @param newTerms to be set
   */
  private void setNewTerms(ShardTerms newTerms) {

    boolean isChanged = false;
    int cnt = 0;
    for (;;)  {
      cnt++;
      log.debug("set new terms {} {}", newTerms, cnt);

      if (log.isDebugEnabled()) log.debug("set new terms {} {}", newTerms, cnt);
      ShardTerms terms = this.terms.get();
      if (terms != null && newTerms.getVersion() <= terms.getVersion() && terms.getTerms().equals(newTerms.getTerms())) {
        // Only skip when there is nothing newer to record. Skipping purely on equal term VALUES while
        // ignoring a higher znode version (e.g. after refreshTerms following a BadVersion) leaves the
        // in-memory version stale, so every subsequent optimistic CAS write reuses the old version and
        // fails with BadVersionException forever -> registerTerm/saveTerms livelock (observed as a
        // multi-second spin of 1M+ retries during restore's burst replica registration).
        return;
      }

      if (terms == null || newTerms.getVersion() > terms.getVersion())  {
        if (this.terms.compareAndSet(terms, newTerms))  {
          log.debug("terms set");
          isChanged = true;
          break;
        }
      } else  {
        break;
      }
    }

    // Notify listeners (e.g. RecoveringCoreTermWatcher) when terms actually changed — including when
    // THIS node wrote them via saveTerms. Without this, a replica whose term the leader just pushed
    // below its own (because the leader skipped forwarding an update to it) is only told to recover
    // via the ZK-watch round-trip, which is asynchronous and can coalesce away under rapid term
    // churn — leaving the replica ACTIVE but silently behind (lost updates / distrib-query undercount).
    if (isChanged) scheduleTermNotify(false);
  }

  private void onTermUpdates(ShardTerms newTerms) {
    try {
      listeners.values().forEach(coreTermWatcher -> coreTermWatcher.onTermChanged(newTerms));
    } catch (Exception e) {
      log.error("Error calling shard term listener", e);
    }
  }
}

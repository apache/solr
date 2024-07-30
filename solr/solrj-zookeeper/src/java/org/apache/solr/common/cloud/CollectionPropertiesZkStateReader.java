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
package org.apache.solr.common.cloud;

import static java.util.Collections.emptyMap;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader.CollectionWatch;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fetches and manages collection properties from a ZooKeeper ensemble */
public class CollectionPropertiesZkStateReader implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private volatile boolean closed = false;

  private final SolrZkClient zkClient;
  private final ZkStateReader zkStateReader;

  /** Collection properties being actively watched */
  private final ConcurrentHashMap<String, VersionedCollectionProps> watchedCollectionProps =
      new ConcurrentHashMap<>();

  /**
   * Manages ZooKeeper watchers for each collection. These watchers monitor changes to the
   * properties of the collection in ZooKeeper. When a change is detected in ZooKeeper, the watcher
   * triggers an update, which then notifies the relevant "collectionPropsObserver".
   */
  private final ConcurrentHashMap<String, PropsWatcher> collectionPropsWatchers =
      new ConcurrentHashMap<>();

  /**
   * Manages a list of observers (listeners) for each collection. These observers need to be
   * notified when the properties of the collection change. When a collection's properties change,
   * all registered observers for that collection are notified by a "collectionPropWatcher".
   */
  private ConcurrentHashMap<String, CollectionWatch<CollectionPropsWatcher>>
      collectionPropsObservers = new ConcurrentHashMap<>();

  /** Used to submit notifications to Collection Properties watchers in order */
  private final ExecutorService collectionPropsNotifications =
      ExecutorUtil.newMDCAwareSingleThreadExecutor(
          new SolrNamedThreadFactory("collectionPropsNotifications"));

  private final ExecutorService notifications =
      ExecutorUtil.newMDCAwareCachedThreadPool("cachecleaner");

  // only kept to identify if the cleaner has already been started.
  private Future<?> collectionPropsCacheCleaner;

  public CollectionPropertiesZkStateReader(ZkStateReader zkStateReader) {
    this.zkClient = zkStateReader.getZkClient();
    this.zkStateReader = zkStateReader;
  }

  /**
   * Get and cache collection properties for a given collection. If the collection is watched, or
   * still cached simply return it from the cache, otherwise fetch it directly from zookeeper and
   * retain the value for at least cacheForMillis milliseconds. Cached properties are watched in
   * zookeeper and updated automatically. This version of {@code getCollectionProperties} should be
   * used when properties need to be consulted frequently in the absence of an active {@link
   * CollectionPropsWatcher}.
   *
   * @param collection The collection for which properties are desired
   * @param cacheForMillis The minimum number of milliseconds to maintain a cache for the specified
   *     collection's properties. Setting a {@code CollectionPropsWatcher} will override this value
   *     and retain the cache for the life of the watcher. A lack of changes in zookeeper may allow
   *     the caching to remain for a greater duration up to the cycle time of {@code CacheCleaner}.
   *     Passing zero for this value will explicitly remove the cached copy if and only if it is due
   *     to expire and no watch exists. Any positive value will extend the expiration time if
   *     required.
   * @return a map representing the key/value properties for the collection.
   */
  public Map<String, String> getCollectionProperties(final String collection, long cacheForMillis) {
    synchronized (watchedCollectionProps) { // synchronized on the specific collection
      Watcher watcher = null;
      if (cacheForMillis > 0) {
        watcher =
            collectionPropsWatchers.compute(
                collection,
                (c, w) ->
                    w == null ? new PropsWatcher(c, cacheForMillis) : w.renew(cacheForMillis));
      }
      VersionedCollectionProps vprops = watchedCollectionProps.get(collection);
      boolean haveUnexpiredProps = vprops != null && vprops.cacheUntilNs > System.nanoTime();
      long untilNs =
          System.nanoTime() + TimeUnit.NANOSECONDS.convert(cacheForMillis, TimeUnit.MILLISECONDS);
      Map<String, String> properties;
      if (haveUnexpiredProps) {
        properties = vprops.props;
        vprops.cacheUntilNs = Math.max(vprops.cacheUntilNs, untilNs);
      } else {
        try {
          VersionedCollectionProps vcp = fetchCollectionProperties(collection, watcher);
          properties = vcp.props;
          if (cacheForMillis > 0) {
            vcp.cacheUntilNs = untilNs;
            watchedCollectionProps.put(collection, vcp);
          } else {
            // we're synchronized on watchedCollectionProps and we can only get here if we have
            // found an expired vprops above, so it is safe to remove the cached value and let the
            // GC free up some mem a bit sooner.
            if (!collectionPropsObservers.containsKey(collection)) {
              watchedCollectionProps.remove(collection);
            }
          }
        } catch (Exception e) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Error reading collection properties",
              SolrZkClient.checkInterrupted(e));
        }
      }
      return properties;
    }
  }

  @Override
  public void close() {
    this.closed = true;
    notifications.shutdownNow();
    ExecutorUtil.shutdownAndAwaitTermination(notifications);
    ExecutorUtil.shutdownAndAwaitTermination(collectionPropsNotifications);
  }

  private static class VersionedCollectionProps {
    int zkVersion;
    Map<String, String> props;
    long cacheUntilNs = 0;

    VersionedCollectionProps(int zkVersion, Map<String, String> props) {
      this.zkVersion = zkVersion;
      this.props = props;
    }
  }

  /** Watches collection properties */
  class PropsWatcher implements Watcher {
    private final String coll;
    private long watchUntilNs;

    PropsWatcher(String coll) {
      this.coll = coll;
      watchUntilNs = 0;
    }

    PropsWatcher(String coll, long forMillis) {
      this.coll = coll;
      watchUntilNs =
          System.nanoTime() + TimeUnit.NANOSECONDS.convert(forMillis, TimeUnit.MILLISECONDS);
    }

    public PropsWatcher renew(long forMillis) {
      watchUntilNs =
          System.nanoTime() + TimeUnit.NANOSECONDS.convert(forMillis, TimeUnit.MILLISECONDS);
      return this;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }

      boolean expired = System.nanoTime() > watchUntilNs;
      if (!collectionPropsObservers.containsKey(coll) && expired) {
        // No one can be notified of the change, we can ignore it and "unset" the watch
        log.debug("Ignoring property change for collection {}", coll);
        return;
      }

      log.info(
          "A collection property change: [{}] for collection [{}] has occurred - updating...",
          event,
          coll);

      refreshAndWatch(true);
    }

    /**
     * Refresh collection properties from ZK and leave a watch for future changes. Updates the
     * properties in watchedCollectionProps with the results of the refresh. Optionally notifies
     * watchers
     */
    void refreshAndWatch(boolean notifyWatchers) {
      try {
        synchronized (watchedCollectionProps) { // making decisions based on the result of a get...
          VersionedCollectionProps vcp = fetchCollectionProperties(coll, this);
          Map<String, String> properties = vcp.props;
          VersionedCollectionProps existingVcp = watchedCollectionProps.get(coll);
          if (existingVcp == null
              || // never called before, record what we found
              vcp.zkVersion > existingVcp.zkVersion
              || // newer info we should update
              vcp.zkVersion == -1) { // node was deleted start over
            watchedCollectionProps.put(coll, vcp);
            if (notifyWatchers) {
              notifyPropsWatchers(coll, properties);
            }
            if (vcp.zkVersion == -1 && existingVcp != null) { // Collection DELETE detected

              // We should not be caching a collection that has been deleted.
              watchedCollectionProps.remove(coll);

              // core ref counting not relevant here, don't need canRemove(), we just sent
              // a notification of an empty set of properties, no reason to watch what doesn't
              // exist.
              collectionPropsObservers.remove(coll);

              // This is the one time we know it's safe to throw this out. We just failed to set the
              // watch due to an NoNodeException, so it isn't held by ZK and can't re-set itself due
              // to an update.
              collectionPropsWatchers.remove(coll);
            }
          }
        }
      } catch (KeeperException.SessionExpiredException
          | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: ", e);
      } catch (KeeperException e) {
        log.error("Lost collection property watcher for {} due to ZK error", coll, e);
        throw new ZooKeeperException(
            SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error(
            "Lost collection property watcher for {} due to the thread being interrupted", coll, e);
      }
    }
  }

  public void registerCollectionPropsWatcher(
      final String collection, CollectionPropsWatcher propsWatcher) {
    AtomicBoolean watchSet = new AtomicBoolean(false);
    collectionPropsObservers.compute(
        collection,
        (k, v) -> {
          if (v == null) {
            v = new CollectionWatch<>();
            watchSet.set(true);
          }
          v.stateWatchers.add(propsWatcher);
          return v;
        });

    if (watchSet.get()) {
      collectionPropsWatchers.computeIfAbsent(collection, PropsWatcher::new).refreshAndWatch(false);
    }
  }

  protected void refreshCollectionProperties() {
    collectionPropsObservers.forEach(
        (k, v) -> {
          collectionPropsWatchers.computeIfAbsent(k, PropsWatcher::new).refreshAndWatch(true);
        });
  }

  public static String getCollectionPropsPath(final String collection) {
    return ZkStateReader.COLLECTIONS_ZKNODE
        + '/'
        + collection
        + '/'
        + ZkStateReader.COLLECTION_PROPS_ZKNODE;
  }

  private VersionedCollectionProps fetchCollectionProperties(String collection, Watcher watcher)
      throws KeeperException, InterruptedException {
    final String znodePath = getCollectionPropsPath(collection);
    // lazy init cache cleaner once we know someone is using collection properties.
    if (collectionPropsCacheCleaner == null) {
      synchronized (zkStateReader.getUpdateLock()) { // Double-checked locking
        if (collectionPropsCacheCleaner == null) {
          collectionPropsCacheCleaner = notifications.submit(new CacheCleaner());
        }
      }
    }
    while (true) {
      try {
        Stat stat = new Stat();
        byte[] data = zkClient.getData(znodePath, watcher, stat, true);
        @SuppressWarnings("unchecked")
        Map<String, String> props = (Map<String, String>) Utils.fromJSON(data);
        return new VersionedCollectionProps(stat.getVersion(), props);
      } catch (ClassCastException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unable to parse collection properties for collection " + collection,
            e);
      } catch (KeeperException.NoNodeException e) {
        if (watcher != null) {
          // Leave an exists watch in place in case a collectionprops.json is created later.
          Stat exists = zkClient.exists(znodePath, watcher, true);
          if (exists != null) {
            // Rare race condition, we tried to fetch the data and couldn't find it, then we found
            // it exists. Loop and try again.
            continue;
          }
        }
        return new VersionedCollectionProps(-1, emptyMap());
      }
    }
  }

  private void notifyPropsWatchers(String collection, Map<String, String> properties) {
    try {
      collectionPropsNotifications.submit(new PropsNotification(collection, properties));
    } catch (RejectedExecutionException e) {
      if (!closed) {
        log.error("Couldn't run collection properties notifications for {}", collection, e);
      }
    }
  }

  private class PropsNotification implements Runnable {

    private final String collection;
    private final Map<String, String> collectionProperties;
    private final List<CollectionPropsWatcher> watchers = new ArrayList<>();

    private PropsNotification(String collection, Map<String, String> collectionProperties) {
      this.collection = collection;
      this.collectionProperties = collectionProperties;
      // guarantee delivery of notification regardless of what happens to collectionPropsObservers
      // while we wait our turn in the executor by capturing the list on creation.
      collectionPropsObservers.compute(
          collection,
          (k, v) -> {
            if (v == null) return null;
            watchers.addAll(v.stateWatchers);
            return v;
          });
    }

    @Override
    public void run() {
      for (CollectionPropsWatcher watcher : watchers) {
        if (watcher.onStateChanged(collectionProperties)) {
          removeCollectionPropsWatcher(collection, watcher);
        }
      }
    }
  }

  private class CacheCleaner implements Runnable {
    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          // Executor shutdown will send us an interrupt
          break;
        }
        watchedCollectionProps
            .entrySet()
            .removeIf(
                entry ->
                    entry.getValue().cacheUntilNs < System.nanoTime()
                        && !collectionPropsObservers.containsKey(entry.getKey()));
      }
    }
  }

  public void removeCollectionPropsWatcher(String collection, CollectionPropsWatcher watcher) {
    collectionPropsObservers.compute(
        collection,
        (k, v) -> {
          if (v == null) return null;
          v.stateWatchers.remove(watcher);
          if (v.canBeRemoved()) {
            // don't want this to happen in middle of other blocks that might add it back.
            synchronized (watchedCollectionProps) {
              watchedCollectionProps.remove(collection);
            }
            return null;
          }
          return v;
        });
  }
}

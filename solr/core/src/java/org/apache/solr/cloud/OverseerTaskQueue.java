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

import com.codahale.metrics.Timer;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link ZkDistributedQueue} augmented with helper methods specific to the overseer task queues.
 * Methods specific to this subclass ignore superclass internal state and hit ZK directly.
 * This is inefficient!  But the API on this class is kind of muddy..
 */
public class OverseerTaskQueue extends ZkDistributedQueue {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String RESPONSE_PREFIX = "qnr-" ;
  public static final byte[] BYTES = new byte[0];

  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final AtomicInteger pendingResponses = new AtomicInteger(0);

  public OverseerTaskQueue(SolrZkClient zookeeper, String dir) {
    this(zookeeper, dir, new Stats());
  }

  public OverseerTaskQueue(SolrZkClient zookeeper, String dir, Stats stats) {
    super(zookeeper, dir, stats);
  }

  /**
   * Returns true if the queue contains a task with the specified async id.
   */
  public boolean containsTaskWithRequestId(String requestIdKey, String requestId)
      throws KeeperException, InterruptedException {

    List<String> childNames = zookeeper.getChildren(dir, null, true);
    stats.setQueueLength(childNames.size());
    for (String childName : childNames) {
      if (childName != null && childName.startsWith(PREFIX)) {
        try {
          byte[] data = zookeeper.getData(dir + '/' + childName, null, (Stat) null, true);
          if (data != null) {
            ZkNodeProps message = ZkNodeProps.load(data);
            if (message.containsKey(requestIdKey)) {
              if (log.isDebugEnabled()) {
                log.debug("Looking for {}, found {}", message.get(requestIdKey), requestId);
              }
              if(message.get(requestIdKey).equals(requestId)) return true;
            }
          }
        } catch (KeeperException.NoNodeException e) {
          // Another client removed the node first, try next
        }
      }
    }

    return false;
  }

  /**
   * Remove the event and save the response into the other path.
   */
  public void remove(QueueEvent event) throws KeeperException,
      InterruptedException {
    Timer.Context time = stats.time(dir + "_remove_event");
    try {
      String path = event.getId();
      String responsePath = dir + "/" + RESPONSE_PREFIX
          + path.substring(path.lastIndexOf('-') + 1);

      try {
        zookeeper.setData(responsePath, event.getBytes(), true);
      } catch (KeeperException.NoNodeException ignored) {
        // we must handle the race case where the node no longer exists
        log.info("Response ZK path: {} doesn't exist. Requestor may have disconnected from ZooKeeper", responsePath);
      }
      try {
        zookeeper.delete(path, -1, true);
      } catch (KeeperException.NoNodeException ignored) {
      }
    } finally {
      time.stop();
    }
  }

  /**
   * Watcher that blocks until a WatchedEvent occurs for a znode.
   */
  static final class LatchWatcher implements Watcher, Closeable {

    private final Lock lock;
    private final Condition bytesRecieved;
    private final String path;
    private final SolrZkClient zkClient;
    private volatile WatchedEvent event;
    private volatile byte[] bytes;

    LatchWatcher(String path, SolrZkClient zkClient) {
      this.lock = new ReentrantLock(false);
      this.bytesRecieved = lock.newCondition();
      this.path = path;
      this.zkClient = zkClient;
    }


    @Override
    public void process(WatchedEvent event) {
      if (Event.EventType.None.equals(event.getType()) || Event.EventType.DataWatchRemoved.equals(event.getType()) ) {
        return;
      }
      ParWork.submitIO("getRespData", () -> {
        try {
          byte[] foundBytes = zkClient.getData(path, this, (Stat) null, true);
          if (foundBytes != null) {
            lock.lock();
            try {
              this.bytes = foundBytes;
              this.event = event;
              bytesRecieved.signalAll();
            } finally {
              lock.unlock();
            }
          }
        } catch (KeeperException.NoNodeException e) {

        } catch (Exception e) {
          log.error("Unexpected exception", e);
        }

      });

    }

    public void await(long timeoutMs) throws KeeperException, InterruptedException {

      try {
        zkClient.exists(path, this, true);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }


      TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);

      lock.lock();
      try {
        while (!timeout.hasTimedOut()) {
          try {
            bytesRecieved.awaitNanos(1000000000);

            if (bytes != null) {
              return;
            }
          } catch (InterruptedException e) {

          }
        }
        if (timeout.hasTimedOut()) {
          log.error("Timeout waiting for response after {}ms", timeout.timeElapsed(TimeUnit.MILLISECONDS));
        }
      } finally {
        lock.unlock();
      }
    }

    public WatchedEvent getWatchedEvent() {
      return event;
    }

    @Override
    public void close() {
      // the watch may still fire, but we are using unique node id's, so save the cost of the remove

//      try {
//        zkClient.removeWatches(path, this, Watcher.WatcherType.Any, true);
//      }  catch (KeeperException.NoWatcherException | AlreadyClosedException e) {
//
//      } catch (Exception e) {
//        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
//      }
    }

    public byte[] getData() {
      return bytes;
    }
  }

  /**
   * Inserts data into zookeeper.
   *
   * @return true if data was successfully added
   */
  private String createData(String path, byte[] data, CreateMode mode)
      throws KeeperException, InterruptedException {

      try {
        return zookeeper.create(path, data, mode, true);
      } catch (KeeperException.NodeExistsException e) {
        log.warn("Found request node already, waiting to see if it frees up ...");
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
  }

  private void createDataAsync(String path, byte[] data, CreateMode mode)
      throws KeeperException, InterruptedException {

    zookeeper.create(path, data, mode, (rc, path1, ctx, name, stat) -> {

      if (rc != 0) {
        KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
        log.error("Exception createDataAsync path={}", path1, e);
      }
    });


  }

  /**
   * Offer the data and wait for the response
   *
   */
  public QueueEvent offer(byte[] data, long timeout) throws KeeperException,
      InterruptedException {
    if (log.isDebugEnabled()) log.debug("offer operation to the Overseer queue {}", Utils.fromJSON(data));

   // Timer.Context time = stats.time(dir + "_offer");
    LatchWatcher watcher = null;
    try {


     // if (log.isDebugEnabled()) log.debug("watchId for response node {}, setting a watch ... ", watchID);

      // create the request node
      CreatePath requestNodeCreate = createRequestNode(data);

      String requestNode = requestNodeCreate.path;

      String responsePath = Overseer.OVERSEER_COLLECTION_MAP_COMPLETED + "/" + OverseerTaskQueue.RESPONSE_PREFIX + requestNode.substring(requestNode.lastIndexOf(
          '-') + 1);

      watcher = new LatchWatcher(responsePath, zookeeper);

      if (log.isDebugEnabled()) log.debug("created request node");

      pendingResponses.incrementAndGet();
      if (log.isDebugEnabled()) log.debug("wait on latch {}", timeout);
      watcher.await(timeout);

      QueueEvent event =  new QueueEvent(responsePath, watcher.getData(), watcher.getWatchedEvent());

      log.debug("deleting request and response node and returning {}", requestNode);


      try {
        Set<String> paths = new HashSet<>(2);
        paths.add(requestNode);
        paths.add(responsePath);
        zookeeper.delete(paths, false, true);
      } catch (KeeperException.NoNodeException e) {
        log.info("request / response node is already missing for {}", requestNode);
      } catch (Exception e) {
        log.warn("Exception trying to delete request / response node " + "requestNode" + " responsePath", e);
      }

      return event;
    } finally {
     // time.stop();
      pendingResponses.decrementAndGet();
      IOUtils.closeQuietly(watcher);
    }
  }

  public static class CreatePath {
    String path;
  }

  CreatePath createRequestNode(byte[] data) throws KeeperException, InterruptedException {
    CreatePath createPath = new CreatePath();

    createPath.path = zookeeper.create(dir + "/" + PREFIX, data, CreateMode.EPHEMERAL_SEQUENTIAL, true, true);

    return createPath;
  }

//  String createResponseNode() throws KeeperException, InterruptedException {
//    return createData(
//        Overseer.OVERSEER_COLLECTION_MAP_COMPLETED + "/" + RESPONSE_PREFIX,
//        null, CreateMode.PERSISTENT_SEQUENTIAL);
//  }

  private static void printQueueEventsListElementIds(ArrayList<QueueEvent> topN) {
    if (log.isDebugEnabled() && !topN.isEmpty()) {
      StringBuilder sb = new StringBuilder("[");
      for (QueueEvent queueEvent : topN) {
        sb.append(queueEvent.getId()).append(", ");
      }
      sb.append("]");
      log.debug("Returning topN elements: {}", sb);
    }
  }

  public static class QueueEvent {
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((id == null) ? 0 : id.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      QueueEvent other = (QueueEvent) obj;
      if (id == null) {
        if (other.id != null) return false;
      } else if (!id.equals(other.id)) return false;
      return true;
    }

    private final WatchedEvent event;
    private final String id;
    private volatile byte[] bytes;

    QueueEvent(String id, byte[] bytes, WatchedEvent event) {
      this.id = id;
      this.bytes = bytes;
      this.event = event;
    }

    public String getId() {
      return id;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public WatchedEvent getWatchedEvent() {
      return event;
    }
  }
}

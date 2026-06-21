package org.apache.solr.cloud.overseer;

import org.apache.solr.cloud.DoNotWrap;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public abstract class QueueWatcher extends DoNotWrap implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final CoreContainer cc;
  protected final ZkController zkController;
  protected final String path;
  protected final Overseer overseer;

  protected volatile boolean closed;
  protected final ReentrantLock ourLock = new ReentrantLock(false);

  public QueueWatcher(CoreContainer cc, Overseer overseer, String path) throws KeeperException {
    this.cc = cc;
    this.zkController = overseer.getZkController();
    this.overseer = overseer;
    this.path = path;
  }

  public abstract void start(boolean weAreReplacement) throws KeeperException, InterruptedException;


  protected abstract Set<Integer> processQueueItems(Queue<String> items, boolean onStart, boolean weAreReplacement);


  @Override
  public void close() {
    this.closed = true;
    closeWatcher();
  }

  private void closeWatcher() {
    try {
      zkController.getZkClient().removeWatches(path, this, Watcher.WatcherType.Any, true);
    } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

    } catch (Exception e) {
      log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
    }
  }
}

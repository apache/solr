package org.apache.solr.cloud;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public abstract class DoNotWrap implements Watcher {

  @Override
  public final void process(WatchedEvent event) {
    processEvent(event);
  }


  public abstract void processEvent(WatchedEvent event);
}

package org.apache.solr.common.cloud;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StateUpdates<I extends Integer, A extends AtomicInteger> extends ConcurrentHashMap {

  private final AtomicInteger stateUpdatesVersion = new AtomicInteger(-1);

  public StateUpdates() {

  }

//  public StateUpdates(Map existingUpdates) {
//    putAll(existingUpdates);
//  }

  public void setStateUpdatesVersion(int version) {
    this.stateUpdatesVersion.set(version);
  }

  public int getStateUpdatesVersion() {
    return this.stateUpdatesVersion.get();
  }

  @Override public String toString() {
    Map<Integer,Character> sortMap = new TreeMap<>();
    this.forEach((k, v) -> {
      sortMap.put((Integer) k, Replica.State.shortStateToLetterState(((AtomicInteger) v).get()));
    });

    return "StateUpdates [" + sortMap + ']';
  }
}

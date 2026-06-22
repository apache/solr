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

  // Value-aware hashCode/equals. The values are AtomicInteger, which inherits identity hashCode/equals, and
  // DocCollection.updateState mutates an existing entry in place (sateForReplica.set(state)). With the default
  // ConcurrentHashMap hashCode (which XORs each value's identity hash) an in-place DOWN->RECOVERING->ACTIVE->LEADER
  // flip leaves the map hashCode unchanged, so clients using getStateUpdates().hashCode() as a freshness token
  // (BaseCloudSolrClient, ZkStateReader) treat stale state as fresh and keep routing on the old replica view.
  // Hash/compare on the AtomicInteger's current int value so a value-only change is visible.
  @Override public int hashCode() {
    int h = 0;
    for (Object o : entrySet()) {
      Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      Object v = e.getValue();
      int vh = v instanceof AtomicInteger ? ((AtomicInteger) v).get() : (v == null ? 0 : v.hashCode());
      h += (e.getKey() == null ? 0 : e.getKey().hashCode()) ^ vh;
    }
    return h;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof StateUpdates)) return false;
    StateUpdates<?, ?> other = (StateUpdates<?, ?>) o;
    if (size() != other.size()) return false;
    for (Object obj : entrySet()) {
      Map.Entry<?, ?> e = (Map.Entry<?, ?>) obj;
      Object ov = other.get(e.getKey());
      if (ov == null && !other.containsKey(e.getKey())) return false;
      int a = e.getValue() instanceof AtomicInteger ? ((AtomicInteger) e.getValue()).get() : -1;
      int b = ov instanceof AtomicInteger ? ((AtomicInteger) ov).get() : -1;
      if (a != b) return false;
    }
    return true;
  }

  @Override public String toString() {
    Map<Integer,Character> sortMap = new TreeMap<>();
    this.forEach((k, v) -> {
      sortMap.put((Integer) k, Replica.State.shortStateToLetterState(((AtomicInteger) v).get()));
    });

    return "StateUpdates [" + sortMap + ']';
  }
}

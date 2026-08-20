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

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * Implementations are expected to implement a correct hashCode and equals method needed to uniquely
 * identify the listener as listeners are managed in a Set. In addition, your listener
 * implementation should call
 * org.apache.solr.cloud.ZkController#removeOnReconnectListener(OnReconnect) when it no longer needs
 * to be notified of ZK reconnection events.
 *
 * <p>When registered as a Curator {@link ConnectionStateListener}, {@link #onReconnect()} runs only
 * after a session expiration ({@link ConnectionState#LOST} then {@link
 * ConnectionState#RECONNECTED}). A reconnect after {@link ConnectionState#SUSPENDED} keeps the
 * ZooKeeper session and must not trigger SolrCloud recovery. See SOLR-18298.
 */
public interface OnReconnect extends ConnectionStateListener {
  void onReconnect();

  @Override
  default void stateChanged(CuratorFramework client, ConnectionState newState) {
    if (newState == ConnectionState.LOST) {
      LostSessions.mark(this);
    } else if (newState == ConnectionState.RECONNECTED && LostSessions.consume(this)) {
      onReconnect();
    }
  }

  /** Tracks listeners that have observed {@link ConnectionState#LOST} and still need reconnect. */
  final class LostSessions {
    private static final Set<OnReconnect> lost = Collections.newSetFromMap(new WeakHashMap<>());

    private LostSessions() {}

    static synchronized void mark(OnReconnect listener) {
      lost.add(listener);
    }

    static synchronized boolean consume(OnReconnect listener) {
      return lost.remove(listener);
    }
  }
}

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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * Listener for ZooKeeper session loss.
 *
 * <p>When registered as a Curator {@link ConnectionStateListener}, {@link #onDisconnect(boolean)}
 * runs only after session expiration ({@link ConnectionState#LOST}). A transient {@link
 * ConnectionState#SUSPENDED} keeps the session and must not tear down SolrCloud leadership. See
 * SOLR-18298.
 */
public interface OnDisconnect extends ConnectionStateListener {
  void onDisconnect(boolean sessionExpired);

  @Override
  default void stateChanged(CuratorFramework client, ConnectionState newState) {
    if (newState == ConnectionState.LOST) {
      onDisconnect(true);
    }
  }
}

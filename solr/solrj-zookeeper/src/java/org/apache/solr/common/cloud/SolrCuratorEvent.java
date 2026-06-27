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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

public enum SolrCuratorEvent {
  // Only triggered after resume from expiration
  EXPIRED_RECONNECTION {
    @Override
    public ConnectionStateListener of(EventAction action) {
      return new ConnectionStateListener() {
        private final AtomicBoolean isExpired = new AtomicBoolean(false);

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
          if (newState == ConnectionState.LOST) {
            isExpired.set(true);
          } else if (newState == ConnectionState.RECONNECTED) {
            if (isExpired.compareAndSet(true, false)) {
              action.respond();
            }
          }
        }
      };
    }
  },

  SESSION_EXPIRATION {
    @Override
    public ConnectionStateListener of(EventAction action) {
      return (client, newState) -> {
        if (newState == ConnectionState.LOST) {
          action.respond();
        }
      };
    }
  };

  public abstract ConnectionStateListener of(EventAction action);

  public interface EventAction {
    void respond();
  }
}

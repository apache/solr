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

import java.io.IOException;
import org.apache.solr.common.cloud.DefaultConnectionStrategy;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Connection strategy that creates instances of {@link TestableZooKeeper} instead of plain {@link
 * ZooKeeper} objects. Useful for adding pause and disconnect events.
 */
public class TestConnectionStrategy extends DefaultConnectionStrategy {
  @Override
  protected ZooKeeper newZooKeeperInstance(
      String serverAddress, int zkClientTimeout, Watcher watcher) throws IOException {
    return new TestableZooKeeper(serverAddress, zkClientTimeout, watcher);
  }
}

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

import java.util.Set;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollectionWatcher;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;

// does not yet mock zkclient at all
public class MockZkStateReader extends ZkStateReader {

  private final Set<String> liveNodes;
  private final ClusterState clusterState;

  public MockZkStateReader(ClusterState clusterState, Set<String> liveNodes) {
    super(new MockSolrZkClient());

    this.clusterState = clusterState;
    this.liveNodes = liveNodes;
  }

  public ClusterState getClusterState() {
    return clusterState;
  }


  public Set<String> getLiveNodes() {
    return liveNodes;
  }

  @Override
  public void registerDocCollectionWatcher(String collection, DocCollectionWatcher docCollectionWatcher) {
    // the doc collection will never be changed by this mock
    // so we just call onStateChanged once with the existing DocCollection object an return
    docCollectionWatcher.onStateChanged(clusterState.getCollection(collection));
  }

  public DocCollection getCollectionOrNull(String collection) {
    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll;
  }

  public DocCollection getCollection(String collection) {
    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll == null) throw new IllegalArgumentException();
    return coll;
  }

  @Override
  public Replica getLeaderRetry(String collection, String shard) {
    // The real implementation relies on ZK watchers / waitForState which this mock has no backend for.
    // Resolve the leader directly from the mock cluster state instead.
    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll == null) return null;
    Slice slice = coll.getSlice(shard);
    if (slice == null) return null;
    return slice.getLeader();
  }
}

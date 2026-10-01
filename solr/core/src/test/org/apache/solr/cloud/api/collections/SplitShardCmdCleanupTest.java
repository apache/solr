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
package org.apache.solr.cloud.api.collections;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.junit.BeforeClass;
import org.junit.Test;

/** In-memory coverage for {@link SplitShardCmd} failed-split state rollback. */
public class SplitShardCmdCleanupTest extends SolrTestCase {

  private static final String COLLECTION = "collection1";
  private static final String PARENT = "shard1";
  private static final String CHILD_0 = "shard1_0";
  private static final String CHILD_1 = "shard1_1";
  private static final List<String> CHILDREN = List.of(CHILD_0, CHILD_1);

  @BeforeClass
  public static void beforeClass() {
    assumeWorkingMockito();
  }

  @Test
  public void testCleanupIncludesParentWhenSnapshotStillShowsActive() {
    DocCollection coll =
        collection(
            slice(PARENT, Slice.State.ACTIVE),
            slice(CHILD_0, Slice.State.CONSTRUCTION),
            slice(CHILD_1, Slice.State.RECOVERY));

    Map<String, Object> updates =
        SplitShardCmd.buildCleanupShardStateUpdates(coll, PARENT, CHILDREN);

    assertNotNull(updates);
    assertEquals(Slice.State.ACTIVE.toString(), updates.get(PARENT));
    assertEquals(Slice.State.CONSTRUCTION.toString(), updates.get(CHILD_0));
    assertEquals(Slice.State.CONSTRUCTION.toString(), updates.get(CHILD_1));
  }

  @Test
  public void testCleanupRestoresInactiveParentWhenChildrenAreNotAllActive() {
    DocCollection coll =
        collection(
            slice(PARENT, Slice.State.INACTIVE),
            slice(CHILD_0, Slice.State.ACTIVE),
            slice(CHILD_1, Slice.State.CONSTRUCTION));

    Map<String, Object> updates =
        SplitShardCmd.buildCleanupShardStateUpdates(coll, PARENT, CHILDREN);

    assertNotNull(updates);
    assertEquals(Slice.State.ACTIVE.toString(), updates.get(PARENT));
    assertEquals(Slice.State.CONSTRUCTION.toString(), updates.get(CHILD_0));
    assertEquals(Slice.State.CONSTRUCTION.toString(), updates.get(CHILD_1));
  }

  @Test
  public void testCleanupSkipsCommittedSwitchOver() {
    DocCollection coll =
        collection(
            slice(PARENT, Slice.State.INACTIVE),
            slice(CHILD_0, Slice.State.ACTIVE),
            slice(CHILD_1, Slice.State.ACTIVE));

    assertNull(SplitShardCmd.buildCleanupShardStateUpdates(coll, PARENT, CHILDREN));
  }

  @Test
  public void testStaleCleanupWithoutParentLeavesParentInactive() {
    ClusterState afterSuccess = apply(initialState(), successSwitch());
    assertSame(
        Slice.State.INACTIVE, afterSuccess.getCollection(COLLECTION).getSlice(PARENT).getState());
    assertSame(
        Slice.State.ACTIVE, afterSuccess.getCollection(COLLECTION).getSlice(CHILD_0).getState());

    ClusterState afterStaleCleanup = apply(afterSuccess, staleCleanupWithoutParent());
    assertSame(
        "omitting parent=active after the success switch is the race",
        Slice.State.INACTIVE,
        afterStaleCleanup.getCollection(COLLECTION).getSlice(PARENT).getState());
    assertSame(
        Slice.State.CONSTRUCTION,
        afterStaleCleanup.getCollection(COLLECTION).getSlice(CHILD_0).getState());
  }

  @Test
  public void testFixedCleanupRestoresParentAfterQueuedSuccessSwitch() {
    DocCollection staleSnapshot = initialState().getCollection(COLLECTION);
    Map<String, Object> cleanup =
        SplitShardCmd.buildCleanupShardStateUpdates(staleSnapshot, PARENT, CHILDREN);
    assertNotNull(cleanup);
    assertEquals(Slice.State.ACTIVE.toString(), cleanup.get(PARENT));

    ClusterState afterSuccess = apply(initialState(), successSwitch());
    ClusterState afterCleanup = apply(afterSuccess, cleanup);

    assertSame(
        Slice.State.ACTIVE, afterCleanup.getCollection(COLLECTION).getSlice(PARENT).getState());
    assertSame(
        Slice.State.CONSTRUCTION,
        afterCleanup.getCollection(COLLECTION).getSlice(CHILD_0).getState());
    assertSame(
        Slice.State.CONSTRUCTION,
        afterCleanup.getCollection(COLLECTION).getSlice(CHILD_1).getState());
  }

  private static ClusterState initialState() {
    return cluster(
        collection(
            slice(PARENT, Slice.State.ACTIVE),
            slice(CHILD_0, Slice.State.CONSTRUCTION),
            slice(CHILD_1, Slice.State.CONSTRUCTION)));
  }

  private static Map<String, Object> successSwitch() {
    Map<String, Object> props = baseUpdate();
    props.put(PARENT, Slice.State.INACTIVE.toString());
    props.put(CHILD_0, Slice.State.ACTIVE.toString());
    props.put(CHILD_1, Slice.State.ACTIVE.toString());
    return props;
  }

  private static Map<String, Object> staleCleanupWithoutParent() {
    Map<String, Object> props = baseUpdate();
    props.put(CHILD_0, Slice.State.CONSTRUCTION.toString());
    props.put(CHILD_1, Slice.State.CONSTRUCTION.toString());
    return props;
  }

  private static Map<String, Object> baseUpdate() {
    Map<String, Object> props = new HashMap<>();
    props.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
    props.put(ZkStateReader.COLLECTION_PROP, COLLECTION);
    return props;
  }

  private static ClusterState apply(ClusterState state, Map<String, Object> update) {
    SolrCloudManager cloudManager = mock(SolrCloudManager.class);
    when(cloudManager.getDistribStateManager()).thenReturn(mock(DistribStateManager.class));
    when(cloudManager.getTimeSource()).thenReturn(TimeSource.NANO_TIME);
    ZkWriteCommand cmd =
        new SliceMutator(cloudManager).updateShardState(state, new ZkNodeProps(update));
    return new ClusterState(Set.of(), Map.of(COLLECTION, cmd.collection));
  }

  private static ClusterState cluster(DocCollection collection) {
    return new ClusterState(Set.of(), Map.of(COLLECTION, collection));
  }

  private static Slice slice(String name, Slice.State state) {
    return new Slice(
        name, Map.of(), Map.of(ZkStateReader.STATE_PROP, state.toString()), COLLECTION);
  }

  private static DocCollection collection(Slice... slices) {
    Map<String, Slice> map = new LinkedHashMap<>();
    for (Slice slice : slices) {
      map.put(slice.getName(), slice);
    }
    return DocCollection.create(
        COLLECTION,
        map,
        Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
        DocRouter.DEFAULT,
        1,
        Instant.EPOCH,
        null);
  }
}

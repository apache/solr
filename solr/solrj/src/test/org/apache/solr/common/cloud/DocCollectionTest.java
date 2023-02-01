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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

public class DocCollectionTest extends SolrTestCaseJ4 {

  @Test
  public void testDocCollectionEqualsAndHashcode() throws Exception {
    String collName = "testcollection";
    String sliceName = "shard1";
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(ZkStateReader.NODE_NAME_PROP, "localhost:8983_solr");
    propMap.put(ZkStateReader.CORE_NAME_PROP, "replicacore");
    propMap.put(ZkStateReader.REPLICA_TYPE, "NRT");
    propMap.put(ZkStateReader.BASE_URL_PROP, "https://localhost");
    Replica replica = new Replica("replica1", propMap, collName, sliceName);
    Map<String, Replica> replicaMap = new HashMap<>();
    replicaMap.put("replica1core", replica);
    Slice slice = new Slice(sliceName, replicaMap, null, collName);
    Map<String, Slice> sliceMap = new HashMap<>();
    sliceMap.put(sliceName, slice);
    DocRouter docRouter = new CompositeIdRouter();
    DocCollection docCollection = new DocCollection(collName, sliceMap, propMap, docRouter, 1);

    DocCollection docCollection2 = new DocCollection(collName, sliceMap, propMap, docRouter, 1);
    String prsState = "replicacore:1:A:L";
    List<String> prsStates = new ArrayList<>();
    prsStates.add(prsState);
    PerReplicaStates prs = new PerReplicaStates(collName, 1, prsStates);
    docCollection2 = docCollection2.copyWith(prs);

    assertFalse("collection'equal method should NOT be same", docCollection.equals(docCollection2));
    assertFalse(
        "collection's hashcode method should NOT be same",
        docCollection.hashCode() == docCollection2.hashCode());
  }

  /** Now we have indent size 0 for any json object serialization */
  @Test
  public void testDocCollectionSeriallizationNoIndent() throws Exception {
    String collName = "test1";
    int numShards = 2048;
    Map<String, Slice> sliceMap = new HashMap<>();
    for (int i = 0; i < numShards; i++) {
      String sliceName = "shard" + i;
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(ZkStateReader.NODE_NAME_PROP, "localhost:8983_solr");
      propMap.put(ZkStateReader.CORE_NAME_PROP, "replicacore");
      propMap.put(ZkStateReader.BASE_URL_PROP, "https://localhost");
      propMap.put(ZkStateReader.STATE_PROP, "active");
      propMap.put(ZkStateReader.REPLICA_TYPE, "NRT");
      propMap.put(ZkStateReader.FORCE_SET_STATE_PROP, "false");
      propMap.put(ZkStateReader.LEADER_PROP, "true");
      Replica replica =
          new Replica(collName + "_" + sliceName + "_replica_n" + i, propMap, collName, sliceName);
      Map<String, Replica> replicaMap = new HashMap<>();
      replicaMap.put("core_node" + i, replica);
      Map<String, Object> shardProps = new HashMap<>();
      shardProps.put(ZkStateReader.SHARD_RANGE_PROP, "7c000000-7c1fffff");
      shardProps.put("state", "active");
      Slice slice = new Slice(sliceName, replicaMap, shardProps, collName);

      sliceMap.put(sliceName, slice);
    }
    DocRouter docRouter = new CompositeIdRouter();

    Map<String, Object> propMap = new HashMap<>();
    // propMap.put(ZkStateReader.BASE_URL_PROP, "https://localhost");
    DocCollection docCollection = new DocCollection(collName, sliceMap, propMap, docRouter, 1);

    byte[] ser = Utils.toJSON(docCollection);

    // sometime it takes url schems http or https - test setup issue
    assertTrue(
        "byte size is wrong at " + ser.length, 540512 == ser.length || 540512 + 2048 == ser.length);
  }
}

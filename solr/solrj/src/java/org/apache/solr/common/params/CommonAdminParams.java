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
package org.apache.solr.common.params;

public interface CommonAdminParams {

  /** Async or not? * */
  String ASYNC = "async";

  /** Wait for final state of the operation. */
  @Deprecated(since = "9.10")
  String WAIT_FOR_FINAL_STATE = "waitForFinalState";

  /**
   * Node-level system property controlling the default value of {@link #WAIT_FOR_FINAL_STATE} when
   * a request omits it. The per-command literal default it overrides differs by command: CREATE,
   * CREATESHARD, SPLITSHARD, ADDREPLICA and MOVEREPLICA default to {@code true}; BALANCE_REPLICAS,
   * MIGRATE_REPLICAS and REPLACENODE keep the pre-10.1 {@code false} default, since each can affect
   * an arbitrary number of replicas cluster-wide. Setting this property (either value) overrides
   * the per-command default uniformly for all 8, mirroring {@code
   * CreateCollectionCmd.PRS_DEFAULT_PROP}.
   */
  String WAIT_FOR_FINAL_STATE_DEFAULT_PROP = "solr.cloud.waitForFinalState.enabled";

  /** Allow in-place move of replicas that use shared filesystems. */
  String IN_PLACE_MOVE = "inPlaceMove";

  /** Method to use for shard splitting. */
  String SPLIT_METHOD = "splitMethod";

  /** Key to use during shard splitting */
  String SPLIT_KEY = "split.key";

  /** Check distribution of documents to prefixes in shard to determine how to split */
  String SPLIT_BY_PREFIX = "splitByPrefix";

  /** Number of sub-shards to create. * */
  String NUM_SUB_SHARDS = "numSubShards";

  /** Timeout for replicas to become active. */
  String TIMEOUT = "timeout";

  /** Inexact shard splitting factor. */
  String SPLIT_FUZZ = "splitFuzz";
}

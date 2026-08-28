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

import static org.apache.solr.common.params.CollectionAdminParams.CALLING_LOCK_ID_HEADER;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.request.SolrQueryRequest;

public class AdminCmdContext {
  private final CollectionParams.CollectionAction action;
  private final String asyncId;
  private String lockId;
  private String callingLockId;
  private ClusterState clusterState;

  public AdminCmdContext(CollectionParams.CollectionAction action) {
    this(action, null);
  }

  public AdminCmdContext(CollectionParams.CollectionAction action, String asyncId) {
    this.action = action;
    this.asyncId = asyncId;
  }

  public AdminCmdContext(
      CollectionParams.CollectionAction action, String asyncId, SolrQueryRequest req) {
    this.action = action;
    this.asyncId = asyncId;
    this.withCallingLockId((String) req.getContext().get(CALLING_LOCK_ID_HEADER));
  }

  public CollectionParams.CollectionAction getAction() {
    return action;
  }

  public String getAsyncId() {
    return asyncId;
  }

  public AdminCmdContext withLockId(String lockId) {
    this.lockId = lockId;
    return this;
  }

  public String getLockId() {
    return lockId;
  }

  public AdminCmdContext withCallingLockId(String callingLockId) {
    this.callingLockId = callingLockId;
    return this;
  }

  public String getCallingLockId() {
    return callingLockId;
  }

  public ClusterState getClusterState() {
    return clusterState;
  }

  public AdminCmdContext withClusterState(ClusterState clusterState) {
    this.clusterState = clusterState;
    return this;
  }

  public AdminCmdContext subRequestContext(CollectionParams.CollectionAction action) {
    return subRequestContext(action, asyncId);
  }

  public AdminCmdContext subRequestContext(
      CollectionParams.CollectionAction action, String asyncId) {
    return new AdminCmdContext(action, asyncId)
        .withCallingLockId(callingLockId)
        .withLockId(lockId)
        .withClusterState(clusterState);
  }
}

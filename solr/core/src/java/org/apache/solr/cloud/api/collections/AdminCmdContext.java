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

import static org.apache.solr.common.params.CollectionAdminParams.CALLING_LOCK_IDS_HEADER;

import java.util.Collections;
import java.util.List;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;

public class AdminCmdContext {
  private final CollectionParams.CollectionAction action;
  private final String asyncId;
  private String lockId;
  private String callingLockIds;
  private String subRequestCallingLockIds;
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
    this.setCallingLockIds((String) req.getContext().get(CALLING_LOCK_IDS_HEADER));
  }

  public CollectionParams.CollectionAction getAction() {
    return action;
  }

  public String getAsyncId() {
    return asyncId;
  }

  public void setLockId(String lockId) {
    this.lockId = lockId;
    regenerateSubRequestCallingLockIds();
  }

  public String getLockId() {
    return lockId;
  }

  public void setCallingLockIds(String callingLockIds) {
    this.callingLockIds = callingLockIds;
    regenerateSubRequestCallingLockIds();
  }

  private void regenerateSubRequestCallingLockIds() {
    if (StrUtils.isNotBlank(callingLockIds) && StrUtils.isNotBlank(lockId)) {
      subRequestCallingLockIds += "," + lockId;
    } else if (StrUtils.isNotBlank(callingLockIds)) {
      subRequestCallingLockIds = callingLockIds;
    } else if (StrUtils.isNotBlank(lockId)) {
      subRequestCallingLockIds = lockId;
    } else {
      subRequestCallingLockIds = null;
    }
  }

  public String getCallingLockIds() {
    return callingLockIds;
  }

  public List<String> getCallingLockIdList() {
    return parseCallingLockIds(callingLockIds);
  }

  public static List<String> parseCallingLockIds(String callingLockIdsString) {
    if (StrUtils.isBlank(callingLockIdsString)) {
      return Collections.emptyList();
    } else {
      return List.of(callingLockIdsString.split(","));
    }
  }

  public ClusterState getClusterState() {
    return clusterState;
  }

  public AdminCmdContext withClusterState(ClusterState clusterState) {
    this.clusterState = clusterState;
    return this;
  }

  public String getSubRequestCallingLockIds() {
    return subRequestCallingLockIds;
  }

  public AdminCmdContext subRequestContext(CollectionParams.CollectionAction action) {
    return subRequestContext(action, asyncId);
  }

  public AdminCmdContext subRequestContext(
      CollectionParams.CollectionAction action, String asyncId) {
    AdminCmdContext nextContext = new AdminCmdContext(action, asyncId);
    nextContext.setCallingLockIds(subRequestCallingLockIds);
    return nextContext.withClusterState(clusterState);
  }
}

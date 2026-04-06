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

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionParams;

public class AdminCmdContext {
  private final CollectionParams.CollectionAction action;
  private final String asyncId;
  private ClusterState clusterState;

  public AdminCmdContext(CollectionParams.CollectionAction action) {
    this(action, null);
  }

  public AdminCmdContext(CollectionParams.CollectionAction action, String asyncId) {
    this.action = action;
    this.asyncId = asyncId;
  }

  public CollectionParams.CollectionAction getAction() {
    return action;
  }

  public String getAsyncId() {
    return asyncId;
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
    AdminCmdContext nextContext = new AdminCmdContext(action, asyncId);
    return nextContext.withClusterState(clusterState);
  }
}

package org.apache.solr.cloud.api.collections;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionParams;
import java.util.List;

public class AdminCmdContext {
  final private CollectionParams.CollectionAction action;
  final private String asyncId;
  private String lockId;
  private List<String> callingLockIds;
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

  public void setLockId(String lockId) {
    this.lockId = lockId;
  }

  public String getLockId() {
    return lockId;
  }

  public void setCallingLockIds(List<String> callingLockIds) {
    this.callingLockIds = callingLockIds;
  }

  public List<String> getCallingLockIds() {
    return callingLockIds;
  }

  public ClusterState getClusterState() {
    return clusterState;
  }

  public void setClusterState(ClusterState clusterState) {
    this.clusterState = clusterState;
  }
}

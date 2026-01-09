package org.apache.solr.cloud.api.collections;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.StrUtils;
import java.util.ArrayList;
import java.util.List;

public class AdminCmdContext {
  final private CollectionParams.CollectionAction action;
  final private String asyncId;
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
    subRequestCallingLockIds = callingLockIds;
    if (StrUtils.isNotBlank(callingLockIds) && StrUtils.isNotBlank(lockId)) {
      subRequestCallingLockIds += ",";
    }
    subRequestCallingLockIds += lockId;
  }

  public String getCallingLockIds() {
    return callingLockIds;
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

  public AdminCmdContext subRequestContext(CollectionParams.CollectionAction action, String asyncId) {
    AdminCmdContext nextContext = new AdminCmdContext(action, asyncId);
    nextContext.setCallingLockIds(subRequestCallingLockIds);
    return nextContext.withClusterState(clusterState);
  }
}

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

package org.apache.solr.handler.admin;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.handler.admin.api.BackupCoreAPI;
import org.apache.solr.handler.api.V2ApiUtils;

class BackupCoreOp implements CoreAdminHandler.CoreAdminOp {

  @Override
  public boolean isExpensive() {
    return true;
  }

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody =
        new BackupCoreAPI.BackupCoreRequestBody();
    backupCoreRequestBody.repository = params.get(CoreAdminParams.BACKUP_REPOSITORY);
    backupCoreRequestBody.location = params.get(CoreAdminParams.BACKUP_LOCATION);
    // An optional parameter to describe the snapshot to be backed-up. If this
    // parameter is not supplied, the latest index commit is backed-up.
    backupCoreRequestBody.commitName = params.get(CoreAdminParams.COMMIT_NAME);

    String cname = params.required().get(CoreAdminParams.CORE);
    backupCoreRequestBody.backupName = parseBackupName(params);
    boolean incremental = isIncrementalBackup(params);
    if (incremental) {
      backupCoreRequestBody.shardBackupId = params.required().get(CoreAdminParams.SHARD_BACKUP_ID);
      backupCoreRequestBody.prevShardBackupId =
          params.get(CoreAdminParams.PREV_SHARD_BACKUP_ID, null);
      backupCoreRequestBody.incremental = true;
    }
    BackupCoreAPI backupCoreAPI =
        new BackupCoreAPI(
            it.handler.coreContainer, it.req, it.rsp, it.handler.coreAdminAsyncTracker);
    try {
      final var response = backupCoreAPI.createBackup(cname, backupCoreRequestBody);
      NamedList<Object> namedList = new SimpleOrderedMap<>();
      V2ApiUtils.squashIntoNamedListWithoutHeader(namedList, response);
      it.rsp.addResponse(namedList);

    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed to backup core=" + cname + " because " + e,
          e);
    }
  }

  /*
   * 'shardBackupId' is required iff 'incremental=true', but unused otherwise
   */
  private ShardBackupId parseShardBackupId(SolrParams params) {
    if (isIncrementalBackup(params)) {
      return ShardBackupId.from(params.required().get(CoreAdminParams.SHARD_BACKUP_ID));
    }
    return null;
  }

  /*
   * 'name' parameter is required iff 'incremental=false', but unused otherwise.
   */
  private String parseBackupName(SolrParams params) {
    if (isIncrementalBackup(params)) {
      return null;
    }
    return params.required().get(CoreAdminParams.NAME);
  }

  private boolean isIncrementalBackup(SolrParams params) {
    return params.getBool(CoreAdminParams.BACKUP_INCREMENTAL, true);
  }
}

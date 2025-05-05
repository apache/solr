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

import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.BACKUPCORE;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.CREATE;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.CREATESNAPSHOT;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.DELETESNAPSHOT;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.INSTALLCOREDATA;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.LISTSNAPSHOTS;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.MERGEINDEXES;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.OVERSEEROP;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.PREPRECOVERY;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REJOINLEADERELECTION;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.RELOAD;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.RENAME;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTBUFFERUPDATES;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTRECOVERY;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTSTATUS;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTSYNCSHARD;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.RESTORECORE;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.SPLIT;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.STATUS;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.SWAP;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.UNLOAD;
import static org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;

import java.lang.invoke.MethodHandles;
import org.apache.solr.client.api.endpoint.SwapCoresApi;
import org.apache.solr.client.api.model.ListCoreSnapshotsResponse;
import org.apache.solr.client.api.model.ReloadCoreRequestBody;
import org.apache.solr.client.api.model.RenameCoreRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SwapCoresRequestBody;
import org.apache.solr.client.api.model.UnloadCoreRequestBody;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler.CoreAdminOp;
import org.apache.solr.handler.admin.api.CoreSnapshot;
import org.apache.solr.handler.admin.api.CreateCore;
import org.apache.solr.handler.admin.api.GetNodeCommandStatus;
import org.apache.solr.handler.admin.api.ReloadCore;
import org.apache.solr.handler.admin.api.RenameCore;
import org.apache.solr.handler.admin.api.SwapCores;
import org.apache.solr.handler.admin.api.UnloadCore;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.update.UpdateLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ImmutableEnumChecker")
public enum CoreAdminOperation implements CoreAdminOp {
  CREATE_OP(
      CREATE,
      it -> {
        final var createParams = CreateCore.createRequestBodyFromV1Params(it.req.getParams());
        final var createCoreApi =
            new CreateCore(
                it.handler.coreContainer, it.handler.getCoreAdminAsyncTracker(), it.req, it.rsp);
        final var response = createCoreApi.createCore(createParams);
        V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
      }),
  UNLOAD_OP(
      UNLOAD,
      it -> {
        SolrParams params = it.req.getParams();
        String cname = params.required().get(CoreAdminParams.CORE);
        final var unloadCoreRequestBody = new UnloadCoreRequestBody();
        unloadCoreRequestBody.deleteIndex = params.getBool(CoreAdminParams.DELETE_INDEX, false);
        unloadCoreRequestBody.deleteDataDir =
            params.getBool(CoreAdminParams.DELETE_DATA_DIR, false);
        unloadCoreRequestBody.deleteInstanceDir =
            params.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, false);
        UnloadCore unloadCoreAPI =
            new UnloadCore(
                it.handler.coreContainer, it.handler.getCoreAdminAsyncTracker(), it.req, it.rsp);
        SolrJerseyResponse response = unloadCoreAPI.unloadCore(cname, unloadCoreRequestBody);
        V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
      }),
  RELOAD_OP(
      RELOAD,
      it -> {
        SolrParams params = it.req.getParams();
        String cname = params.required().get(CoreAdminParams.CORE);

        ReloadCore reloadCoreAPI =
            new ReloadCore(
                it.req, it.rsp, it.handler.coreContainer, it.handler.getCoreAdminAsyncTracker());
        ReloadCoreRequestBody reloadCoreRequestBody = new ReloadCoreRequestBody();
        SolrJerseyResponse response = reloadCoreAPI.reloadCore(cname, reloadCoreRequestBody);
        V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
      }),
  STATUS_OP(STATUS, new StatusOp()),
  SWAP_OP(
      SWAP,
      it -> {
        final SolrParams params = it.req.getParams();
        final String cname = params.required().get(CoreAdminParams.CORE);
        final var swapCoresRequestBody = new SwapCoresRequestBody();
        swapCoresRequestBody.with = params.required().get(CoreAdminParams.OTHER);
        ;
        SwapCoresApi swapCoresApi =
            new SwapCores(
                it.handler.coreContainer, it.handler.getCoreAdminAsyncTracker(), it.req, it.rsp);
        SolrJerseyResponse response = swapCoresApi.swapCores(cname, swapCoresRequestBody);
        V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
      }),

  RENAME_OP(
      RENAME,
      it -> {
        SolrParams params = it.req.getParams();
        final String cname = params.required().get(CoreAdminParams.CORE);
        final String name = params.required().get(CoreAdminParams.OTHER);
        final var renameCoreRequestBody = new RenameCoreRequestBody();
        renameCoreRequestBody.to = name;
        final var renameCoreApi =
            new RenameCore(
                it.handler.coreContainer, it.handler.getCoreAdminAsyncTracker(), it.req, it.rsp);
        SolrJerseyResponse response = renameCoreApi.renameCore(cname, renameCoreRequestBody);
        V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
      }),

  MERGEINDEXES_OP(MERGEINDEXES, new MergeIndexesOp()),

  SPLIT_OP(SPLIT, new SplitOp()),

  PREPRECOVERY_OP(PREPRECOVERY, new PrepRecoveryOp()),

  REQUESTRECOVERY_OP(
      REQUESTRECOVERY,
      it -> {
        final SolrParams params = it.req.getParams();
        final String cname = params.required().get(CoreAdminParams.CORE);
        log().info("It has been requested that we recover: core=" + cname);

        try (SolrCore core = it.handler.coreContainer.getCore(cname)) {
          if (core != null) {
            // This can take a while, but doRecovery is already async so don't worry about it here
            core.getUpdateHandler()
                .getSolrCoreState()
                .doRecovery(it.handler.coreContainer, core.getCoreDescriptor());
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to locate core " + cname);
          }
        }
      }),
  REQUESTSYNCSHARD_OP(REQUESTSYNCSHARD, new RequestSyncShardOp()),

  REQUESTBUFFERUPDATES_OP(
      REQUESTBUFFERUPDATES,
      it -> {
        SolrParams params = it.req.getParams();
        String cname = params.required().get(CoreAdminParams.NAME);
        log().info("Starting to buffer updates on core:" + cname);

        try (SolrCore core = it.handler.coreContainer.getCore(cname)) {
          if (core == null)
            throw new SolrException(ErrorCode.BAD_REQUEST, "Core [" + cname + "] does not exist");
          UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
          if (updateLog.getState() != UpdateLog.State.ACTIVE) {
            throw new SolrException(
                ErrorCode.SERVER_ERROR, "Core " + cname + " not in active state");
          }
          updateLog.bufferUpdates();
          it.rsp.add("core", cname);
          it.rsp.add("status", "BUFFERING");
        } catch (Throwable e) {
          if (e instanceof SolrException) throw (SolrException) e;
          else
            throw new SolrException(ErrorCode.SERVER_ERROR, "Could not start buffering updates", e);
        } finally {
          if (it.req != null) it.req.close();
        }
      }),
  REQUESTAPPLYUPDATES_OP(REQUESTAPPLYUPDATES, new RequestApplyUpdatesOp()),

  REQUESTSTATUS_OP(
      REQUESTSTATUS,
      it -> {
        final var params = it.req.getParams();
        final String requestId = params.required().get(CoreAdminParams.REQUESTID);
        log().info("Checking request status for : " + requestId);

        final var requestCoreCommandStatusApi =
            new GetNodeCommandStatus(
                it.handler.coreContainer, it.handler.coreAdminAsyncTracker, it.req, it.rsp);
        final SolrJerseyResponse response = requestCoreCommandStatusApi.getCommandStatus(requestId);
        V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
      }),

  OVERSEEROP_OP(
      OVERSEEROP,
      it -> {
        ZkController zkController = it.handler.coreContainer.getZkController();
        if (zkController != null) {
          String op = it.req.getParams().get("op");
          String electionNode = it.req.getParams().get("electionNode");
          if (electionNode != null) {
            zkController.rejoinOverseerElection(electionNode, "rejoinAtHead".equals(op));
          } else {
            log().info("electionNode is required param");
          }
        }
      }),

  REJOINLEADERELECTION_OP(
      REJOINLEADERELECTION,
      it -> {
        ZkController zkController = it.handler.coreContainer.getZkController();

        if (zkController != null) {
          zkController.rejoinShardLeaderElection(it.req.getParams());
        } else {
          log()
              .warn(
                  "zkController is null in CoreAdminHandler.handleRequestInternal:REJOINLEADERELECTION. No action taken.");
        }
      }),
  BACKUPCORE_OP(BACKUPCORE, new BackupCoreOp()),
  RESTORECORE_OP(RESTORECORE, new RestoreCoreOp()),
  INSTALLCOREDATA_OP(INSTALLCOREDATA, new InstallCoreDataOp()),
  CREATESNAPSHOT_OP(CREATESNAPSHOT, new CreateSnapshotOp()),
  DELETESNAPSHOT_OP(DELETESNAPSHOT, new DeleteSnapshotOp()),
  @SuppressWarnings({"unchecked"})
  LISTSNAPSHOTS_OP(
      LISTSNAPSHOTS,
      it -> {
        final SolrParams params = it.req.getParams();
        final String coreName = params.required().get(CoreAdminParams.CORE);

        final CoreContainer coreContainer = it.handler.getCoreContainer();
        final CoreSnapshot coreSnapshotAPI =
            new CoreSnapshot(it.req, it.rsp, coreContainer, it.handler.getCoreAdminAsyncTracker());

        final ListCoreSnapshotsResponse response = coreSnapshotAPI.listSnapshots(coreName);

        V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
      });

  final CoreAdminParams.CoreAdminAction action;
  final CoreAdminOp fun;

  CoreAdminOperation(CoreAdminParams.CoreAdminAction action, CoreAdminOp fun) {
    this.action = action;
    this.fun = fun;
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static Logger log() {
    return log;
  }

  @Override
  public boolean isExpensive() {
    // delegates this to the actual implementation
    return fun.isExpensive();
  }

  @Override
  public void execute(CallInfo it) throws Exception {
    try {
      fun.execute(it);
    } catch (SolrException | InterruptedException e) {
      // No need to re-wrap; throw as-is.
      throw e;
    } catch (Exception e) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR, "Error handling '" + action.name() + "' action", e);
    }
  }
}

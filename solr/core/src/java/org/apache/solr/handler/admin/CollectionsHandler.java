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

import static org.apache.solr.client.solrj.response.RequestStatusState.COMPLETED;
import static org.apache.solr.client.solrj.response.RequestStatusState.FAILED;
import static org.apache.solr.client.solrj.response.RequestStatusState.NOT_FOUND;
import static org.apache.solr.client.solrj.response.RequestStatusState.RUNNING;
import static org.apache.solr.client.solrj.response.RequestStatusState.SUBMITTED;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET_SHUFFLE;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.NUM_SLICES;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.REQUESTID;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SHARD_UNIQUE;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_NAME;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_VALUE;
import static org.apache.solr.common.params.CollectionAdminParams.SHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ALIASPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BACKUP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CLUSTERPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CLUSTERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.COLLECTIONPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.COLSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATEALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESNAPSHOT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEBACKUP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETENODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESNAPSHOT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DISTRIBUTEDAPIPROCESSING;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.FORCELEADER;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.INSTALLSHARDDATA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.LIST;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.LISTALIASES;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.LISTBACKUP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.LISTSNAPSHOTS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MIGRATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOCK_COLL_TASK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.OVERSEERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REBALANCELEADERS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REINDEXCOLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RENAME;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REPLACENODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REQUESTSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RESTORE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.SPLITSHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.SYNCSHARD;
import static org.apache.solr.common.params.CollectionParams.SOURCE_NODE;
import static org.apache.solr.common.params.CollectionParams.TARGET_NODE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.IN_PLACE_MOVE;
import static org.apache.solr.common.params.CommonAdminParams.NUM_SUB_SHARDS;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_BY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_FUZZ;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_METHOD;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.TIMING;
import static org.apache.solr.common.params.CommonParams.VALUE_LONG;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.common.params.ShardParams._ROUTE_;
import static org.apache.solr.common.util.StrUtils.formatString;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.AddReplicaPropertyRequestBody;
import org.apache.solr.client.api.model.ReplaceNodeRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpdateAliasPropertiesRequestBody;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerSolrResponseSerializer;
import org.apache.solr.cloud.OverseerTaskQueue;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkController.NotInClusterStateException;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.cloud.api.collections.ReindexCollectionCmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.AddReplicaProperty;
import org.apache.solr.handler.admin.api.AdminAPIBase;
import org.apache.solr.handler.admin.api.AliasProperty;
import org.apache.solr.handler.admin.api.BalanceReplicas;
import org.apache.solr.handler.admin.api.BalanceShardUniqueAPI;
import org.apache.solr.handler.admin.api.CollectionPropertyAPI;
import org.apache.solr.handler.admin.api.CollectionStatusAPI;
import org.apache.solr.handler.admin.api.CreateAliasAPI;
import org.apache.solr.handler.admin.api.CreateCollectionAPI;
import org.apache.solr.handler.admin.api.CreateCollectionBackupAPI;
import org.apache.solr.handler.admin.api.CreateCollectionSnapshotAPI;
import org.apache.solr.handler.admin.api.CreateReplicaAPI;
import org.apache.solr.handler.admin.api.CreateShardAPI;
import org.apache.solr.handler.admin.api.DeleteAlias;
import org.apache.solr.handler.admin.api.DeleteCollection;
import org.apache.solr.handler.admin.api.DeleteCollectionBackup;
import org.apache.solr.handler.admin.api.DeleteCollectionSnapshot;
import org.apache.solr.handler.admin.api.DeleteNode;
import org.apache.solr.handler.admin.api.DeleteReplica;
import org.apache.solr.handler.admin.api.DeleteReplicaProperty;
import org.apache.solr.handler.admin.api.DeleteShardAPI;
import org.apache.solr.handler.admin.api.ForceLeader;
import org.apache.solr.handler.admin.api.InstallShardDataAPI;
import org.apache.solr.handler.admin.api.ListAliases;
import org.apache.solr.handler.admin.api.ListCollectionBackups;
import org.apache.solr.handler.admin.api.ListCollectionSnapshotsAPI;
import org.apache.solr.handler.admin.api.ListCollections;
import org.apache.solr.handler.admin.api.MigrateDocsAPI;
import org.apache.solr.handler.admin.api.MigrateReplicasAPI;
import org.apache.solr.handler.admin.api.ModifyCollectionAPI;
import org.apache.solr.handler.admin.api.MoveReplicaAPI;
import org.apache.solr.handler.admin.api.RebalanceLeadersAPI;
import org.apache.solr.handler.admin.api.ReloadCollectionAPI;
import org.apache.solr.handler.admin.api.RenameCollection;
import org.apache.solr.handler.admin.api.ReplaceNode;
import org.apache.solr.handler.admin.api.RestoreCollectionAPI;
import org.apache.solr.handler.admin.api.SplitShardAPI;
import org.apache.solr.handler.admin.api.SyncShard;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.tracing.TraceUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionsHandler extends RequestHandlerBase implements PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final CoreContainer coreContainer;
  private final Optional<DistributedCollectionConfigSetCommandRunner>
      distributedCollectionConfigSetCommandRunner;

  public CollectionsHandler() {
    // Unlike most request handlers, CoreContainer initialization
    // should happen in the constructor...
    this(null);
  }

  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public CollectionsHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    distributedCollectionConfigSetCommandRunner =
        coreContainer != null
            ? coreContainer.getDistributedCollectionCommandRunner()
            : Optional.empty();
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
    String action = ctx.getParams().get("action");
    if (action == null) return PermissionNameProvider.Name.COLL_READ_PERM;
    CollectionParams.CollectionAction collectionAction =
        CollectionParams.CollectionAction.get(action);
    if (collectionAction == null) return null;
    return collectionAction.isWrite
        ? PermissionNameProvider.Name.COLL_EDIT_PERM
        : PermissionNameProvider.Name.COLL_READ_PERM;
  }

  @Override
  public final void init(NamedList<?> args) {}

  /**
   * The instance of CoreContainer this handler handles. This should be the CoreContainer instance
   * that created this handler.
   *
   * @return a CoreContainer instance
   */
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Make sure the cores is enabled
    CoreContainer cores = checkErrors();

    // Pick the action
    SolrParams params = req.getParams();
    String a = params.get(CoreAdminParams.ACTION);
    if (a == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "action is a required param");
    }
    CollectionAction action = CollectionAction.get(a);
    if (action == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
    }

    // Initial logging/tracing setup
    final String collection = params.get(COLLECTION);
    MDCLoggingContext.setCollection(collection);
    TraceUtils.setDbInstance(req, collection);
    if (log.isDebugEnabled()) {
      log.debug(
          "Invoked Collection Action: {} with params {}", action.toLower(), req.getParamString());
    }

    CollectionOperation operation = CollectionOperation.get(action);
    invokeAction(req, rsp, cores, action, operation);
    rsp.setHttpCaching(false);
  }

  protected CoreContainer checkErrors() {
    CoreContainer cores = getCoreContainer();
    AdminAPIBase.validateZooKeeperAwareCoreContainer(cores);
    return cores;
  }

  @SuppressWarnings({"unchecked"})
  void invokeAction(
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      CoreContainer cores,
      CollectionAction action,
      CollectionOperation operation)
      throws Exception {
    if (!coreContainer.isZooKeeperAware()) {
      throw new SolrException(
          BAD_REQUEST, "Invalid request. collections can be accessed only in SolrCloud mode");
    }
    Map<String, Object> props = operation.execute(req, rsp, this);
    if (props == null) {
      return;
    }

    String asyncId = req.getParams().get(ASYNC);
    if (asyncId != null) {
      props.put(ASYNC, asyncId);
    }

    props.put(QUEUE_OPERATION, operation.action.toLower());

    ZkNodeProps zkProps = new ZkNodeProps(props);
    final SolrResponse overseerResponse;

    overseerResponse = submitCollectionApiCommand(zkProps, operation.action, operation.timeOut);

    rsp.getValues().addAll(overseerResponse.getResponse());
    Exception exp = overseerResponse.getException();
    if (exp != null) {
      rsp.setException(exp);
    }
  }

  static final Set<String> KNOWN_ROLES = Set.of("overseer");

  public static long DEFAULT_COLLECTION_OP_TIMEOUT = 180 * 1000;

  public SolrResponse submitCollectionApiCommand(ZkNodeProps m, CollectionAction action)
      throws KeeperException, InterruptedException {
    return submitCollectionApiCommand(m, action, DEFAULT_COLLECTION_OP_TIMEOUT);
  }

  public static SolrResponse submitCollectionApiCommand(
      CoreContainer coreContainer,
      Optional<DistributedCollectionConfigSetCommandRunner>
          distributedCollectionConfigSetCommandRunner,
      ZkNodeProps m,
      CollectionAction action,
      long timeout)
      throws KeeperException, InterruptedException {
    // Collection API messages are either sent to Overseer and processed there, or processed
    // locally. Distributing Collection API implies we're also distributing Cluster State Updates.
    // Indeed collection creation with non distributed cluster state updates requires for "Per
    // Replica States" that the Collection API be running on Overseer, which means that it is not
    // possible to distributed Collection API while keeping cluster state updates on Overseer. See
    // the call to CollectionCommandContext.submitIntraProcessMessage() in
    // CreateCollectionCmd.call() which can only be done if the Collection API command runs on the
    // same JVM as the Overseer based cluster state update... The configuration handling includes
    // these checks to not allow distributing collection API without distributing cluster state
    // updates (but the other way around is ok). See constructor of CloudConfig.
    if (distributedCollectionConfigSetCommandRunner.isPresent()) {
      return distributedCollectionConfigSetCommandRunner
          .get()
          .runCollectionCommand(m, action, timeout);
    } else { // Sending the Collection API message to Overseer via a Zookeeper queue
      String operation = m.getStr(QUEUE_OPERATION);
      if (operation == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "missing key " + QUEUE_OPERATION);
      }
      if (m.get(ASYNC) != null) {
        String asyncId = m.getStr(ASYNC);
        NamedList<Object> r = new NamedList<>();

        if (coreContainer.getZkController().claimAsyncId(asyncId)) {
          boolean success = false;
          try {
            coreContainer.getZkController().getOverseerCollectionQueue().offer(m);
            success = true;
          } finally {
            if (!success) {
              try {
                coreContainer.getZkController().clearAsyncId(asyncId);
              } catch (Exception e) {
                // let the original exception bubble up
                log.error("Unable to release async ID={}", asyncId, e);
                SolrZkClient.checkInterrupted(e);
              }
            }
          }
        } else {
          throw new SolrException(
              BAD_REQUEST, "Task with the same requestid already exists. (" + asyncId + ")");
        }
        r.add(CoreAdminParams.REQUESTID, m.get(ASYNC));

        return new OverseerSolrResponse(r);
      }

      long time = System.nanoTime();
      QueueEvent event =
          coreContainer
              .getZkController()
              .getOverseerCollectionQueue()
              .offer(Utils.toJSON(m), timeout);
      if (event.getBytes() != null) {
        return OverseerSolrResponseSerializer.deserialize(event.getBytes());
      } else {
        if (System.nanoTime() - time
            >= TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS)) {
          throw new SolrException(
              ErrorCode.SERVER_ERROR,
              operation + " the collection time out:" + timeout / 1000 + "s");
        } else if (event.getWatchedEvent() != null) {
          throw new SolrException(
              ErrorCode.SERVER_ERROR,
              operation
                  + " the collection error [Watcher fired on path: "
                  + event.getWatchedEvent().getPath()
                  + " state: "
                  + event.getWatchedEvent().getState()
                  + " type "
                  + event.getWatchedEvent().getType()
                  + "]");
        } else {
          throw new SolrException(
              ErrorCode.SERVER_ERROR, operation + " the collection unknown case");
        }
      }
    }
  }

  public SolrResponse submitCollectionApiCommand(
      ZkNodeProps m, CollectionAction action, long timeout)
      throws KeeperException, InterruptedException {
    return submitCollectionApiCommand(
        coreContainer, distributedCollectionConfigSetCommandRunner, m, action, timeout);
  }

  private boolean overseerCollectionQueueContains(String asyncId)
      throws KeeperException, InterruptedException {
    OverseerTaskQueue collectionQueue =
        coreContainer.getZkController().getOverseerCollectionQueue();
    return collectionQueue.containsTaskWithRequestId(ASYNC, asyncId);
  }

  /**
   * Copy prefixed params into a map. There must only be one value for these parameters.
   *
   * @param params The source of params from which copies should be made
   * @param props The map into which param names and values should be copied as keys and values
   *     respectively
   * @param prefix The prefix to select.
   * @return the map supplied in the props parameter, modified to contain the prefixed params.
   */
  private static Map<String, Object> copyPropertiesWithPrefix(
      SolrParams params, Map<String, Object> props, String prefix) {
    Iterator<String> iter = params.getParameterNamesIterator();
    while (iter.hasNext()) {
      String param = iter.next();
      if (param.startsWith(prefix)) {
        final String[] values = params.getParams(param);
        if (values.length != 1) {
          throw new SolrException(
              BAD_REQUEST, "Only one value can be present for parameter " + param);
        }
        props.put(param, values[0]);
      }
    }
    return props;
  }

  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Manage SolrCloud Collections";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  private static void addStatusToResponse(
      NamedList<Object> results, RequestStatusState state, String msg) {
    SimpleOrderedMap<String> status = new SimpleOrderedMap<>();
    status.add("state", state.getKey());
    status.add("msg", msg);
    results.add("status", status);
  }

  @SuppressWarnings("ImmutableEnumChecker")
  public enum CollectionOperation implements CollectionOp {
    CREATE_OP(
        CREATE,
        (req, rsp, h) -> {
          final CreateCollectionAPI.CreateCollectionRequestBody requestBody =
              CreateCollectionAPI.CreateCollectionRequestBody.fromV1Params(req.getParams(), true);
          final CreateCollectionAPI createApi = new CreateCollectionAPI(h.coreContainer, req, rsp);
          final SolrJerseyResponse response = createApi.createCollection(requestBody);

          // 'rsp' may be null, as when overseer commands execute CollectionAction impl's directly.
          if (rsp != null) {
            V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);
          }
          return null;
        }),
    COLSTATUS_OP(
        COLSTATUS,
        (req, rsp, h) -> {
          Map<String, Object> props =
              copy(
                  req.getParams(),
                  null,
                  COLLECTION_PROP,
                  ColStatus.CORE_INFO_PROP,
                  ColStatus.SEGMENTS_PROP,
                  ColStatus.FIELD_INFO_PROP,
                  ColStatus.RAW_SIZE_PROP,
                  ColStatus.RAW_SIZE_SUMMARY_PROP,
                  ColStatus.RAW_SIZE_DETAILS_PROP,
                  ColStatus.RAW_SIZE_SAMPLING_PERCENT_PROP,
                  ColStatus.SIZE_INFO_PROP);

          new ColStatus(
                  h.coreContainer.getSolrClientCache(),
                  h.coreContainer.getZkController().getZkStateReader().getClusterState(),
                  new ZkNodeProps(props))
              .getColStatus(rsp.getValues());
          return null;
        }),
    DELETE_OP(
        DELETE,
        (req, rsp, h) -> {
          final RequiredSolrParams requiredParams = req.getParams().required();
          final DeleteCollection deleteCollectionAPI =
              new DeleteCollection(h.coreContainer, req, rsp);
          final SolrJerseyResponse deleteCollResponse =
              deleteCollectionAPI.deleteCollection(
                  requiredParams.get(NAME),
                  req.getParams().getBool(FOLLOW_ALIASES),
                  req.getParams().get(ASYNC));
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, deleteCollResponse);

          return null;
        }),
    // XXX should this command support followAliases?
    RELOAD_OP(
        RELOAD,
        (req, rsp, h) -> {
          ReloadCollectionAPI.invokeFromV1Params(h.coreContainer, req, rsp);
          return null;
        }),

    RENAME_OP(
        RENAME,
        (req, rsp, h) -> {
          Map<String, Object> map =
              copy(req.getParams().required(), null, NAME, CollectionAdminParams.TARGET);
          return copy(req.getParams(), map, FOLLOW_ALIASES);
        }),

    REINDEXCOLLECTION_OP(
        REINDEXCOLLECTION,
        (req, rsp, h) -> {
          Map<String, Object> m = copy(req.getParams().required(), null, NAME);
          copy(
              req.getParams(),
              m,
              ReindexCollectionCmd.COMMAND,
              ReindexCollectionCmd.REMOVE_SOURCE,
              ReindexCollectionCmd.TARGET,
              ZkStateReader.CONFIGNAME_PROP,
              NUM_SLICES,
              NRT_REPLICAS,
              PULL_REPLICAS,
              TLOG_REPLICAS,
              REPLICATION_FACTOR,
              CREATE_NODE_SET,
              CREATE_NODE_SET_SHUFFLE,
              "shards",
              CommonParams.ROWS,
              CommonParams.Q,
              CommonParams.FL,
              FOLLOW_ALIASES);
          if (req.getParams().get("collection." + ZkStateReader.CONFIGNAME_PROP) != null) {
            m.put(
                ZkStateReader.CONFIGNAME_PROP,
                req.getParams().get("collection." + ZkStateReader.CONFIGNAME_PROP));
          }
          copyPropertiesWithPrefix(req.getParams(), m, "router.");
          return m;
        }),

    SYNCSHARD_OP(
        SYNCSHARD,
        (req, rsp, h) -> {
          SyncShard.invokeFromV1Params(h.coreContainer, req, rsp);
          return null;
        }),

    CREATEALIAS_OP(
        CREATEALIAS,
        (req, rsp, h) -> {
          final CreateAliasAPI.CreateAliasRequestBody reqBody =
              CreateAliasAPI.createFromSolrParams(req.getParams());
          final SolrJerseyResponse response =
              new CreateAliasAPI(h.coreContainer, req, rsp).createAlias(reqBody);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);
          return null;
        }),

    DELETEALIAS_OP(
        DELETEALIAS,
        (req, rsp, h) -> {
          final DeleteAlias deleteAliasAPI = new DeleteAlias(h.coreContainer, req, rsp);
          final SolrJerseyResponse response =
              deleteAliasAPI.deleteAlias(
                  req.getParams().required().get(NAME), req.getParams().get(ASYNC));
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);
          return null;
        }),

    /**
     * Change properties for an alias (use CREATEALIAS_OP to change the actual value of the alias)
     */
    ALIASPROP_OP(
        ALIASPROP,
        (req, rsp, h) -> {
          String name = req.getParams().required().get(NAME);
          Map<String, Object> properties = collectToMap(req.getParams(), "property");
          final var requestBody = new UpdateAliasPropertiesRequestBody();
          requestBody.properties = properties;
          requestBody.async = req.getParams().get(ASYNC);
          final AliasProperty aliasPropertyAPI = new AliasProperty(h.coreContainer, req, rsp);
          final SolrJerseyResponse getAliasesResponse =
              aliasPropertyAPI.updateAliasProperties(name, requestBody);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, getAliasesResponse);
          return null;
        }),

    /** List the aliases and associated properties. */
    LISTALIASES_OP(
        LISTALIASES,
        (req, rsp, h) -> {
          final ListAliases getAliasesAPI = new ListAliases(h.coreContainer, req, rsp);
          final SolrJerseyResponse getAliasesResponse = getAliasesAPI.getAliases();
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, getAliasesResponse);
          return null;
        }),
    SPLITSHARD_OP(
        SPLITSHARD,
        DEFAULT_COLLECTION_OP_TIMEOUT * 5,
        (req, rsp, h) -> {
          String name = req.getParams().required().get(COLLECTION_PROP);
          // TODO : add support for multiple shards
          String shard = req.getParams().get(SHARD_ID_PROP);
          String rangesStr = req.getParams().get(CoreAdminParams.RANGES);
          String splitKey = req.getParams().get("split.key");
          String numSubShards = req.getParams().get(NUM_SUB_SHARDS);
          String fuzz = req.getParams().get(SPLIT_FUZZ);

          if (splitKey == null && shard == null) {
            throw new SolrException(
                ErrorCode.BAD_REQUEST, "At least one of shard, or split.key should be specified.");
          }
          if (splitKey != null && shard != null) {
            throw new SolrException(
                ErrorCode.BAD_REQUEST, "Only one of 'shard' or 'split.key' should be specified");
          }
          if (splitKey != null && rangesStr != null) {
            throw new SolrException(
                ErrorCode.BAD_REQUEST, "Only one of 'ranges' or 'split.key' should be specified");
          }
          if (numSubShards != null && (splitKey != null || rangesStr != null)) {
            throw new SolrException(
                ErrorCode.BAD_REQUEST,
                "numSubShards can not be specified with split.key or ranges parameters");
          }
          if (fuzz != null && (splitKey != null || rangesStr != null)) {
            throw new SolrException(
                ErrorCode.BAD_REQUEST,
                "fuzz can not be specified with split.key or ranges parameters");
          }

          Map<String, Object> map =
              copy(
                  req.getParams(),
                  null,
                  COLLECTION_PROP,
                  SHARD_ID_PROP,
                  "split.key",
                  CoreAdminParams.RANGES,
                  WAIT_FOR_FINAL_STATE,
                  TIMING,
                  SPLIT_METHOD,
                  NUM_SUB_SHARDS,
                  SPLIT_FUZZ,
                  SPLIT_BY_PREFIX,
                  FOLLOW_ALIASES,
                  CREATE_NODE_SET_PARAM);
          return copyPropertiesWithPrefix(req.getParams(), map, PROPERTY_PREFIX);
        }),
    DELETESHARD_OP(
        DELETESHARD,
        (req, rsp, h) -> {
          DeleteShardAPI.invokeWithV1Params(h.coreContainer, req, rsp);
          return null;
        }),
    FORCELEADER_OP(
        FORCELEADER,
        (req, rsp, h) -> {
          ForceLeader.invokeFromV1Params(h.coreContainer, req, rsp);
          return null;
        }),
    CREATESHARD_OP(
        CREATESHARD,
        (req, rsp, h) -> {
          CreateShardAPI.invokeFromV1Params(h.coreContainer, req, rsp);
          return null;
        }),
    DELETEREPLICA_OP(
        DELETEREPLICA,
        (req, rsp, h) -> {
          DeleteReplica.invokeWithV1Params(h.coreContainer, req, rsp);
          return null;
        }),
    MIGRATE_OP(
        MIGRATE,
        (req, rsp, h) -> {
          Map<String, Object> map =
              copy(
                  req.getParams().required(),
                  null,
                  COLLECTION_PROP,
                  "split.key",
                  "target.collection");
          return copy(req.getParams(), map, "forward.timeout", FOLLOW_ALIASES);
        }),
    ADDROLE_OP(
        ADDROLE,
        (req, rsp, h) -> {
          Map<String, Object> map = copy(req.getParams().required(), null, "role", "node");
          if (!KNOWN_ROLES.contains(map.get("role")))
            throw new SolrException(
                ErrorCode.BAD_REQUEST, "Unknown role. Supported roles are ," + KNOWN_ROLES);
          return map;
        }),
    REMOVEROLE_OP(
        REMOVEROLE,
        (req, rsp, h) -> {
          Map<String, Object> map = copy(req.getParams().required(), null, "role", "node");
          if (!KNOWN_ROLES.contains(map.get("role")))
            throw new SolrException(
                ErrorCode.BAD_REQUEST, "Unknown role. Supported roles are ," + KNOWN_ROLES);
          return map;
        }),
    CLUSTERPROP_OP(
        CLUSTERPROP,
        (req, rsp, h) -> {
          String name = req.getParams().required().get(NAME);
          String val = req.getParams().get(VALUE_LONG);
          ClusterProperties cp =
              new ClusterProperties(h.coreContainer.getZkController().getZkClient());
          cp.setClusterProperty(name, val);
          return null;
        }),
    COLLECTIONPROP_OP(
        COLLECTIONPROP,
        (req, rsp, h) -> {
          final String collection = req.getParams().required().get(NAME);
          final String propName = req.getParams().required().get(PROPERTY_NAME);
          final String val = req.getParams().get(PROPERTY_VALUE);

          final CollectionPropertyAPI setCollPropApi =
              new CollectionPropertyAPI(h.coreContainer, req, rsp);
          final SolrJerseyResponse setPropRsp =
              (val != null)
                  ? setCollPropApi.createOrUpdateCollectionProperty(
                      collection,
                      propName,
                      new CollectionPropertyAPI.UpdateCollectionPropertyRequestBody(val))
                  : setCollPropApi.deleteCollectionProperty(collection, propName);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, setPropRsp);
          return null;
        }),
    REQUESTSTATUS_OP(
        REQUESTSTATUS,
        (req, rsp, h) -> {
          req.getParams().required().check(REQUESTID);

          final CoreContainer coreContainer = h.coreContainer;
          final String requestId = req.getParams().get(REQUESTID);
          final ZkController zkController = coreContainer.getZkController();

          final NamedList<Object> status = new NamedList<>();
          if (coreContainer.getDistributedCollectionCommandRunner().isEmpty()) {
            if (zkController.getOverseerCompletedMap().contains(requestId)) {
              final byte[] mapEntry = zkController.getOverseerCompletedMap().get(requestId);
              rsp.getValues()
                  .addAll(OverseerSolrResponseSerializer.deserialize(mapEntry).getResponse());
              addStatusToResponse(
                  status, COMPLETED, "found [" + requestId + "] in completed tasks");
            } else if (zkController.getOverseerFailureMap().contains(requestId)) {
              final byte[] mapEntry = zkController.getOverseerFailureMap().get(requestId);
              rsp.getValues()
                  .addAll(OverseerSolrResponseSerializer.deserialize(mapEntry).getResponse());
              addStatusToResponse(status, FAILED, "found [" + requestId + "] in failed tasks");
            } else if (zkController.getOverseerRunningMap().contains(requestId)) {
              addStatusToResponse(status, RUNNING, "found [" + requestId + "] in running tasks");
            } else if (h.overseerCollectionQueueContains(requestId)) {
              addStatusToResponse(
                  status, SUBMITTED, "found [" + requestId + "] in submitted tasks");
            } else {
              addStatusToResponse(
                  status, NOT_FOUND, "Did not find [" + requestId + "] in any tasks queue");
            }
          } else {
            Pair<RequestStatusState, OverseerSolrResponse> sr =
                coreContainer
                    .getDistributedCollectionCommandRunner()
                    .get()
                    .getAsyncTaskRequestStatus(requestId);
            final String message;
            switch (sr.first()) {
              case COMPLETED:
                message = "found [" + requestId + "] in completed tasks";
                rsp.getValues().addAll(sr.second().getResponse());
                break;
              case FAILED:
                message = "found [" + requestId + "] in failed tasks";
                rsp.getValues().addAll(sr.second().getResponse());
                break;
              case RUNNING:
                message = "found [" + requestId + "] in running tasks";
                break;
              case SUBMITTED:
                message = "found [" + requestId + "] in submitted tasks";
                break;
              default:
                message = "Did not find [" + requestId + "] in any tasks queue";
            }
            addStatusToResponse(status, sr.first(), message);
          }

          rsp.getValues().addAll(status);
          return null;
        }),
    DELETESTATUS_OP(
        DELETESTATUS,
        new CollectionOp() {
          @Override
          public Map<String, Object> execute(
              SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
            final CoreContainer coreContainer = h.coreContainer;
            final String requestId = req.getParams().get(REQUESTID);
            final ZkController zkController = coreContainer.getZkController();
            boolean flush = req.getParams().getBool(CollectionAdminParams.FLUSH, false);

            if (requestId == null && !flush) {
              throw new SolrException(
                  ErrorCode.BAD_REQUEST, "Either requestid or flush parameter must be specified.");
            }

            if (requestId != null && flush) {
              throw new SolrException(
                  ErrorCode.BAD_REQUEST,
                  "Both requestid and flush parameters can not be specified together.");
            }

            if (coreContainer.getDistributedCollectionCommandRunner().isEmpty()) {
              if (flush) {
                Collection<String> completed = zkController.getOverseerCompletedMap().keys();
                Collection<String> failed = zkController.getOverseerFailureMap().keys();
                for (String asyncId : completed) {
                  zkController.getOverseerCompletedMap().remove(asyncId);
                  zkController.clearAsyncId(asyncId);
                }
                for (String asyncId : failed) {
                  zkController.getOverseerFailureMap().remove(asyncId);
                  zkController.clearAsyncId(asyncId);
                }
                rsp.getValues()
                    .add("status", "successfully cleared stored collection api responses");
              } else {
                // Request to cleanup
                if (zkController.getOverseerCompletedMap().remove(requestId)) {
                  zkController.clearAsyncId(requestId);
                  rsp.getValues()
                      .add(
                          "status", "successfully removed stored response for [" + requestId + "]");
                } else if (zkController.getOverseerFailureMap().remove(requestId)) {
                  zkController.clearAsyncId(requestId);
                  rsp.getValues()
                      .add(
                          "status", "successfully removed stored response for [" + requestId + "]");
                } else {
                  rsp.getValues()
                      .add("status", "[" + requestId + "] not found in stored responses");
                  // Don't call zkController.clearAsyncId for this, since it could be a
                  // running/pending task
                }
              }
            } else {
              if (flush) {
                coreContainer.getDistributedCollectionCommandRunner().get().deleteAllAsyncIds();
                rsp.getValues()
                    .add("status", "successfully cleared stored collection api responses");
              } else {
                if (coreContainer
                    .getDistributedCollectionCommandRunner()
                    .get()
                    .deleteSingleAsyncId(requestId)) {
                  rsp.getValues()
                      .add(
                          "status", "successfully removed stored response for [" + requestId + "]");
                } else {
                  rsp.getValues()
                      .add("status", "[" + requestId + "] not found in stored responses");
                }
              }
            }
            return null;
          }
        }),
    ADDREPLICA_OP(
        ADDREPLICA,
        (req, rsp, h) -> {
          final var params = req.getParams();
          params.required().check(COLLECTION_PROP, SHARD_ID_PROP);

          final var api = new CreateReplicaAPI(h.coreContainer, req, rsp);
          final var requestBody =
              CreateReplicaAPI.AddReplicaRequestBody.fromV1Params(req.getParams());
          final var response =
              api.createReplica(
                  params.get(COLLECTION_PROP), params.get(SHARD_ID_PROP), requestBody);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);
          return null;
        }),
    OVERSEERSTATUS_OP(OVERSEERSTATUS, (req, rsp, h) -> new LinkedHashMap<>()),
    DISTRIBUTEDAPIPROCESSING_OP(
        DISTRIBUTEDAPIPROCESSING,
        (req, rsp, h) -> {
          NamedList<Object> results = new NamedList<>();
          boolean isDistributedApi =
              h.coreContainer.getDistributedCollectionCommandRunner().isPresent();
          results.add("isDistributedApi", isDistributedApi);
          rsp.getValues().addAll(results);
          return null;
        }),
    /** Handle list collection request. Do list collection request to zk host */
    LIST_OP(
        LIST,
        (req, rsp, h) -> {
          final ListCollections listCollectionsAPI = new ListCollections(h.coreContainer, req, rsp);
          final SolrJerseyResponse listCollectionsResponse = listCollectionsAPI.listCollections();
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, listCollectionsResponse);
          return null;
        }),
    /**
     * Handle cluster status request. Can return status per specific collection/shard or per all
     * collections.
     */
    CLUSTERSTATUS_OP(
        CLUSTERSTATUS,
        (req, rsp, h) -> {
          Map<String, Object> all =
              copy(req.getParams(), null, COLLECTION_PROP, SHARD_ID_PROP, _ROUTE_, "prs");
          new ClusterStatus(
                  h.coreContainer.getZkController().getZkStateReader(), new ZkNodeProps(all))
              .getClusterStatus(rsp.getValues());
          return null;
        }),
    ADDREPLICAPROP_OP(
        ADDREPLICAPROP,
        (req, rsp, h) -> {
          final RequiredSolrParams requiredParams = req.getParams().required();
          final var requestBody = new AddReplicaPropertyRequestBody();
          requestBody.value = requiredParams.get(PROPERTY_VALUE_PROP);
          requestBody.shardUnique = req.getParams().getBool(SHARD_UNIQUE);
          final String propName = requiredParams.get(PROPERTY_PROP);
          final String trimmedPropName =
              propName.startsWith(PROPERTY_PREFIX)
                  ? propName.substring(PROPERTY_PREFIX.length())
                  : propName;

          final AddReplicaProperty addReplicaPropertyAPI =
              new AddReplicaProperty(h.coreContainer, req, rsp);
          final SolrJerseyResponse addReplicaPropResponse =
              addReplicaPropertyAPI.addReplicaProperty(
                  requiredParams.get(COLLECTION_PROP),
                  requiredParams.get(SHARD_ID_PROP),
                  requiredParams.get(REPLICA_PROP),
                  trimmedPropName,
                  requestBody);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, addReplicaPropResponse);
          return null;
        }),
    DELETEREPLICAPROP_OP(
        DELETEREPLICAPROP,
        (req, rsp, h) -> {
          final var api = new DeleteReplicaProperty(h.coreContainer, req, rsp);
          final var deleteReplicaPropResponse =
              DeleteReplicaProperty.invokeUsingV1Inputs(api, req.getParams());
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, deleteReplicaPropResponse);
          return null;
        }),
    // XXX should this command support followAliases?
    BALANCESHARDUNIQUE_OP(
        BALANCESHARDUNIQUE,
        (req, rsp, h) -> {
          BalanceShardUniqueAPI.invokeFromV1Params(h.coreContainer, req, rsp);
          return null;
        }),
    REBALANCELEADERS_OP(
        REBALANCELEADERS,
        (req, rsp, h) -> {
          new RebalanceLeaders(req, rsp, h).execute();
          return null;
        }),
    // XXX should this command support followAliases?
    MODIFYCOLLECTION_OP(
        MODIFYCOLLECTION,
        (req, rsp, h) -> {
          Map<String, Object> m =
              copy(req.getParams(), null, CollectionAdminRequest.MODIFIABLE_COLLECTION_PROPERTIES);
          copyPropertiesWithPrefix(req.getParams(), m, PROPERTY_PREFIX);
          if (m.isEmpty()) {
            throw new SolrException(
                ErrorCode.BAD_REQUEST,
                formatString(
                    "no supported values provided {0}",
                    CollectionAdminRequest.MODIFIABLE_COLLECTION_PROPERTIES.toString()));
          }
          copy(req.getParams().required(), m, COLLECTION_PROP);
          for (Map.Entry<String, Object> entry : m.entrySet()) {
            String prop = entry.getKey();
            if ("".equals(entry.getValue())) {
              // set to an empty string is equivalent to removing the property, see SOLR-12507
              entry.setValue(null);
            }
            DocCollection.verifyProp(m, prop);
          }
          if (m.get(REPLICATION_FACTOR) != null) {
            m.put(NRT_REPLICAS, m.get(REPLICATION_FACTOR));
          }
          return m;
        }),
    BACKUP_OP(
        BACKUP,
        (req, rsp, h) -> {
          final var response =
              CreateCollectionBackupAPI.invokeFromV1Params(req, rsp, h.coreContainer);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);
          return null;
        }),
    RESTORE_OP(
        RESTORE,
        (req, rsp, h) -> {
          final var response = RestoreCollectionAPI.invokeFromV1Params(req, rsp, h.coreContainer);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);
          return null;
        }),
    INSTALLSHARDDATA_OP(
        INSTALLSHARDDATA,
        (req, rsp, h) -> {
          req.getParams().required().check(COLLECTION, SHARD);
          final String collectionName = req.getParams().get(COLLECTION);
          final String shardName = req.getParams().get(SHARD);
          final InstallShardDataAPI.InstallShardRequestBody reqBody =
              new InstallShardDataAPI.InstallShardRequestBody();
          reqBody.asyncId = req.getParams().get(ASYNC);
          reqBody.repository = req.getParams().get(BACKUP_REPOSITORY);
          reqBody.location = req.getParams().get(BACKUP_LOCATION);

          final InstallShardDataAPI installApi = new InstallShardDataAPI(h.coreContainer, req, rsp);
          final SolrJerseyResponse installResponse =
              installApi.installShardData(collectionName, shardName, reqBody);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, installResponse);
          return null;
        }),
    DELETEBACKUP_OP(
        DELETEBACKUP,
        (req, rsp, h) -> {
          DeleteCollectionBackup.invokeFromV1Params(h.coreContainer, req, rsp);
          return null;
        }),
    LISTBACKUP_OP(
        LISTBACKUP,
        (req, rsp, h) -> {
          req.getParams().required().check(NAME);
          ListCollectionBackups.invokeFromV1Params(h.coreContainer, req, rsp);
          return null;
        }),
    CREATESNAPSHOT_OP(
        CREATESNAPSHOT,
        (req, rsp, h) -> {
          req.getParams().required().check(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME);

          final String extCollectionName = req.getParams().get(COLLECTION_PROP);
          final boolean followAliases = req.getParams().getBool(FOLLOW_ALIASES, false);
          final String commitName = req.getParams().get(CoreAdminParams.COMMIT_NAME);
          final String asyncId = req.getParams().get(ASYNC);

          final CreateCollectionSnapshotAPI createCollectionSnapshotAPI =
              new CreateCollectionSnapshotAPI(h.coreContainer, req, rsp);

          final CreateCollectionSnapshotAPI.CreateSnapshotRequestBody requestBody =
              new CreateCollectionSnapshotAPI.CreateSnapshotRequestBody();
          requestBody.followAliases = followAliases;
          requestBody.asyncId = asyncId;

          final CreateCollectionSnapshotAPI.CreateSnapshotResponse createSnapshotResponse =
              createCollectionSnapshotAPI.createSnapshot(
                  extCollectionName, commitName, requestBody);

          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, createSnapshotResponse);

          return null;
        }),
    DELETESNAPSHOT_OP(
        DELETESNAPSHOT,
        (req, rsp, h) -> {
          req.getParams().required().check(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME);

          final String extCollectionName = req.getParams().get(COLLECTION_PROP);
          final String commitName = req.getParams().get(CoreAdminParams.COMMIT_NAME);
          final boolean followAliases = req.getParams().getBool(FOLLOW_ALIASES, false);
          final String asyncId = req.getParams().get(ASYNC);

          final DeleteCollectionSnapshot deleteCollectionSnapshotAPI =
              new DeleteCollectionSnapshot(h.coreContainer, req, rsp);

          final var deleteSnapshotResponse =
              deleteCollectionSnapshotAPI.deleteSnapshot(
                  extCollectionName, commitName, followAliases, asyncId);

          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, deleteSnapshotResponse);
          return null;
        }),
    LISTSNAPSHOTS_OP(
        LISTSNAPSHOTS,
        (req, rsp, h) -> {
          req.getParams().required().check(COLLECTION_PROP);

          final ListCollectionSnapshotsAPI listCollectionSnapshotsAPI =
              new ListCollectionSnapshotsAPI(h.coreContainer, req, rsp);

          final ListCollectionSnapshotsAPI.ListSnapshotsResponse response =
              listCollectionSnapshotsAPI.listSnapshots(req.getParams().get(COLLECTION_PROP));

          NamedList<Object> snapshots = new NamedList<>();
          for (CollectionSnapshotMetaData meta : response.snapshots.values()) {
            snapshots.add(meta.getName(), meta.toNamedList());
          }

          rsp.add(SolrSnapshotManager.SNAPSHOTS_INFO, snapshots);
          return null;
        }),
    REPLACENODE_OP(
        REPLACENODE,
        (req, rsp, h) -> {
          final SolrParams params = req.getParams();
          final RequiredSolrParams requiredParams = req.getParams().required();
          final var requestBody = new ReplaceNodeRequestBody();
          requestBody.targetNodeName = params.get(TARGET_NODE);
          requestBody.waitForFinalState = params.getBool(WAIT_FOR_FINAL_STATE);
          requestBody.async = params.get(ASYNC);
          final ReplaceNode replaceNodeAPI = new ReplaceNode(h.coreContainer, req, rsp);
          final SolrJerseyResponse replaceNodeResponse =
              replaceNodeAPI.replaceNode(requiredParams.get(SOURCE_NODE), requestBody);
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, replaceNodeResponse);
          return null;
        }),
    MOVEREPLICA_OP(
        MOVEREPLICA,
        (req, rsp, h) -> {
          Map<String, Object> map = copy(req.getParams().required(), null, COLLECTION_PROP);

          return copy(
              req.getParams(),
              map,
              CollectionParams.FROM_NODE,
              CollectionParams.SOURCE_NODE,
              TARGET_NODE,
              WAIT_FOR_FINAL_STATE,
              IN_PLACE_MOVE,
              "replica",
              "shard",
              FOLLOW_ALIASES);
        }),
    DELETENODE_OP(
        DELETENODE,
        (req, rsp, h) -> {
          final DeleteNode deleteNodeAPI = new DeleteNode(h.coreContainer, req, rsp);
          final SolrJerseyResponse deleteNodeResponse =
              DeleteNode.invokeUsingV1Inputs(deleteNodeAPI, req.getParams());
          V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, deleteNodeResponse);
          return null;
        }),
    MOCK_COLL_TASK_OP(
        MOCK_COLL_TASK,
        (req, rsp, h) -> {
          Map<String, Object> map = copy(req.getParams().required(), null, COLLECTION_PROP);
          return copy(req.getParams(), map, "sleep");
        });

    /**
     * Collects all prefixed properties in a new map. The resulting keys have the prefix removed.
     *
     * @param params The solr params from which to extract prefixed properties.
     * @param prefix The prefix to identify properties to be extracted
     * @return a map with collected properties
     */
    private static Map<String, Object> collectToMap(SolrParams params, String prefix) {
      Map<String, Object> sink = new LinkedHashMap<>();
      Iterator<String> iter = params.getParameterNamesIterator();
      while (iter.hasNext()) {
        String param = iter.next();
        if (param.startsWith(prefix)) {
          sink.put(param.substring(prefix.length() + 1), params.get(param));
        }
      }
      return sink;
    }

    public final CollectionOp fun;
    final CollectionAction action;
    final long timeOut;

    CollectionOperation(CollectionAction action, CollectionOp fun) {
      this(action, DEFAULT_COLLECTION_OP_TIMEOUT, fun);
    }

    CollectionOperation(CollectionAction action, long timeOut, CollectionOp fun) {
      this.action = action;
      this.timeOut = timeOut;
      this.fun = fun;
    }

    public static CollectionOperation get(CollectionAction action) {
      for (CollectionOperation op : values()) {
        if (op.action == action) return op;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "No such action " + action);
    }

    @Override
    public Map<String, Object> execute(
        SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
      return fun.execute(req, rsp, h);
    }
  }

  public static void waitForActiveCollection(
      String collectionName, CoreContainer cc, SolrResponse createCollResponse)
      throws KeeperException, InterruptedException {

    if (createCollResponse.getResponse().get("exception") != null) {
      // the main called failed, don't wait
      if (log.isInfoEnabled()) {
        log.info(
            "Not waiting for active collection due to exception: {}",
            createCollResponse.getResponse().get("exception"));
      }
      return;
    }

    int replicaFailCount;
    if (createCollResponse.getResponse().get("failure") != null) {
      replicaFailCount = ((NamedList) createCollResponse.getResponse().get("failure")).size();
    } else {
      replicaFailCount = 0;
    }

    CloudConfig ccfg = cc.getConfig().getCloudConfig();
    Integer seconds = ccfg.getCreateCollectionWaitTimeTillActive();
    Boolean checkLeaderOnly = ccfg.isCreateCollectionCheckLeaderActive();
    if (log.isInfoEnabled()) {
      log.info(
          "Wait for new collection to be active for at most {} seconds. Check all shard {}",
          seconds,
          (checkLeaderOnly ? "leaders" : "replicas"));
    }

    try {
      cc.getZkController()
          .getZkStateReader()
          .waitForState(
              collectionName,
              seconds,
              TimeUnit.SECONDS,
              (n, c) -> {
                if (c == null) {
                  // the collection was not created, don't wait
                  return true;
                }

                if (c.getSlices() != null) {
                  Collection<Slice> shards = c.getSlices();
                  int replicaNotAliveCnt = 0;
                  for (Slice shard : shards) {
                    Collection<Replica> replicas;
                    if (!checkLeaderOnly) replicas = shard.getReplicas();
                    else {
                      replicas = new ArrayList<Replica>();
                      replicas.add(shard.getLeader());
                    }
                    for (Replica replica : replicas) {
                      State state = replica.getState();
                      if (log.isDebugEnabled()) {
                        log.debug(
                            "Checking replica status, collection={} replica={} state={}",
                            collectionName,
                            replica.getCoreUrl(),
                            state);
                      }
                      if (!n.contains(replica.getNodeName())
                          || !state.equals(Replica.State.ACTIVE)) {
                        replicaNotAliveCnt++;
                        return false;
                      }
                    }
                  }

                  return (replicaNotAliveCnt == 0) || (replicaNotAliveCnt <= replicaFailCount);
                }
                return false;
              });
    } catch (TimeoutException | InterruptedException e) {

      String error =
          "Timeout waiting for active collection " + collectionName + " with timeout=" + seconds;
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
    }
  }

  interface CollectionOp {
    Map<String, Object> execute(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h)
        throws Exception;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(
        CreateReplicaAPI.class,
        AddReplicaProperty.class,
        BalanceShardUniqueAPI.class,
        CreateAliasAPI.class,
        CreateCollectionAPI.class,
        CreateCollectionBackupAPI.class,
        CreateShardAPI.class,
        DeleteAlias.class,
        DeleteCollectionBackup.class,
        DeleteCollection.class,
        DeleteReplica.class,
        DeleteReplicaProperty.class,
        DeleteShardAPI.class,
        ForceLeader.class,
        InstallShardDataAPI.class,
        ListCollections.class,
        ListCollectionBackups.class,
        ReloadCollectionAPI.class,
        RenameCollection.class,
        ReplaceNode.class,
        MigrateReplicasAPI.class,
        BalanceReplicas.class,
        RestoreCollectionAPI.class,
        SyncShard.class,
        CollectionPropertyAPI.class,
        DeleteNode.class,
        ListAliases.class,
        AliasProperty.class,
        ListCollectionSnapshotsAPI.class,
        CreateCollectionSnapshotAPI.class,
        DeleteCollectionSnapshot.class);
  }

  @Override
  public Collection<Api> getApis() {
    final List<Api> apis = new ArrayList<>();
    apis.addAll(AnnotatedApi.getApis(new SplitShardAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new MigrateDocsAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new ModifyCollectionAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new MoveReplicaAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new RebalanceLeadersAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new CollectionStatusAPI(this)));
    return apis;
  }

  // These "copy" methods were once SolrParams.getAll but were moved here as there is no universal
  // way that a SolrParams can be represented in a Map; there are various choices.

  /** Copy all params to the given map or if the given map is null create a new one */
  static Map<String, Object> copy(
      SolrParams source, Map<String, Object> sink, Collection<String> paramNames) {
    if (sink == null) sink = new LinkedHashMap<>();
    for (String param : paramNames) {
      String[] v = source.getParams(param);
      if (v != null && v.length > 0) {
        if (v.length == 1) {
          sink.put(param, v[0]);
        } else {
          sink.put(param, v);
        }
      }
    }
    return sink;
  }

  /** Copy all params to the given map or if the given map is null create a new one */
  static Map<String, Object> copy(
      SolrParams source, Map<String, Object> sink, String... paramNames) {
    return copy(
        source, sink, paramNames == null ? Collections.emptyList() : Arrays.asList(paramNames));
  }
}

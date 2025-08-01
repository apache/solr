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

import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.STATUS;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.CoreSnapshot;
import org.apache.solr.handler.admin.api.CoreStatus;
import org.apache.solr.handler.admin.api.CreateCore;
import org.apache.solr.handler.admin.api.CreateCoreBackup;
import org.apache.solr.handler.admin.api.GetNodeCommandStatus;
import org.apache.solr.handler.admin.api.InstallCoreData;
import org.apache.solr.handler.admin.api.MergeIndexes;
import org.apache.solr.handler.admin.api.OverseerOperationAPI;
import org.apache.solr.handler.admin.api.PrepareCoreRecoveryAPI;
import org.apache.solr.handler.admin.api.RejoinLeaderElectionAPI;
import org.apache.solr.handler.admin.api.ReloadCore;
import org.apache.solr.handler.admin.api.RenameCore;
import org.apache.solr.handler.admin.api.RequestApplyCoreUpdatesAPI;
import org.apache.solr.handler.admin.api.RequestBufferUpdatesAPI;
import org.apache.solr.handler.admin.api.RequestCoreRecoveryAPI;
import org.apache.solr.handler.admin.api.RequestSyncShardAPI;
import org.apache.solr.handler.admin.api.RestoreCore;
import org.apache.solr.handler.admin.api.SplitCoreAPI;
import org.apache.solr.handler.admin.api.SwapCores;
import org.apache.solr.handler.admin.api.UnloadCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.stats.MetricUtils;
import org.apache.solr.util.tracing.TraceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @since solr 1.3
 */
public class CoreAdminHandler extends RequestHandlerBase implements PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final CoreContainer coreContainer;
  protected final CoreAdminAsyncTracker coreAdminAsyncTracker;
  protected final Map<String, CoreAdminOp> opMap;

  public static String RESPONSE_STATUS = "STATUS";

  public static String RESPONSE_MESSAGE = "msg";
  public static String OPERATION_RESPONSE = "response";

  public CoreAdminHandler() {
    super();
    // Unlike most request handlers, CoreContainer initialization
    // should happen in the constructor...
    this.coreContainer = null;
    this.coreAdminAsyncTracker = new CoreAdminAsyncTracker();
    this.opMap = initializeOpMap();
  }

  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public CoreAdminHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    this.coreAdminAsyncTracker = new CoreAdminAsyncTracker();
    this.opMap = initializeOpMap();
  }

  @Override
  public final void init(NamedList<?> args) {
    throw new SolrException(
        SolrException.ErrorCode.SERVER_ERROR,
        "CoreAdminHandler should not be configured in solrconf.xml\n"
            + "it is a special Handler configured directly by the RequestDispatcher");
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);
    coreAdminAsyncTracker.standardExecutor =
        MetricUtils.instrumentedExecutorService(
            coreAdminAsyncTracker.standardExecutor,
            this,
            solrMetricsContext.getMetricRegistry(),
            SolrMetricManager.mkName(
                "parallelCoreAdminExecutor", getCategory().name(), scope, "threadPool"));

    coreAdminAsyncTracker.expensiveExecutor =
        MetricUtils.instrumentedExecutorService(
            coreAdminAsyncTracker.expensiveExecutor,
            this,
            solrMetricsContext.getMetricRegistry(),
            SolrMetricManager.mkName(
                "parallelCoreExpensiveAdminExecutor", getCategory().name(), scope, "threadPool"));
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  /**
   * Registers custom actions defined in {@code solr.xml}. Called from the {@link CoreContainer}
   * during load process.
   *
   * @param customActions to register
   * @throws SolrException in case of action with indicated name is already registered
   */
  public final void registerCustomActions(Map<String, CoreAdminOp> customActions) {

    for (Entry<String, CoreAdminOp> entry : customActions.entrySet()) {

      String action = entry.getKey().toLowerCase(Locale.ROOT);
      CoreAdminOp operation = entry.getValue();

      if (opMap.containsKey(action)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "CoreAdminHandler already registered action " + action);
      }

      opMap.put(action, operation);
    }
  }

  /**
   * The instance of CoreContainer this handler handles. This should be the CoreContainer instance
   * that created this handler.
   *
   * @return a CoreContainer instance
   */
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  /**
   * The instance of CoreAdminAsyncTracker owned by this handler.
   *
   * @return a {@link CoreAdminAsyncTracker} instance.
   */
  public CoreAdminAsyncTracker getCoreAdminAsyncTracker() {
    return coreAdminAsyncTracker;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Make sure the cores is enabled
    try {
      CoreContainer cores = getCoreContainer();
      if (cores == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Core container instance missing");
      }
      // boolean doPersist = false;
      final String taskId = req.getParams().get(CommonAdminParams.ASYNC);

      // Pick the action
      final String action = req.getParams().get(ACTION, STATUS.toString()).toLowerCase(Locale.ROOT);
      CoreAdminOp op = opMap.get(action);
      if (op == null) {
        log.warn(
            "action '{}' not found, calling custom action handler. "
                + "If original intention was to target some custom behaviour "
                + "use custom actions defined in 'solr.xml' instead",
            action);
        handleCustomAction(req, rsp);
        return;
      }

      final CallInfo callInfo = new CallInfo(this, req, rsp, op);
      final String coreName =
          req.getParams().get(CoreAdminParams.CORE, req.getParams().get(CoreAdminParams.NAME));
      MDCLoggingContext.setCoreName(coreName);
      TraceUtils.setDbInstance(req, coreName);
      if (taskId == null) {
        callInfo.call();
      } else {
        Callable<SolrQueryResponse> task =
            () -> {
              callInfo.call();
              return callInfo.rsp;
            };

        var taskObject =
            new CoreAdminAsyncTracker.TaskObject(taskId, action, op.isExpensive(), task);

        coreAdminAsyncTracker.submitAsyncTask(taskObject);
      }
    } finally {
      rsp.setHttpCaching(false);
    }
  }

  /**
   * Handle Custom Action.
   *
   * <p>This method could be overridden by derived classes to handle custom actions. <br>
   * By default - this method throws a solr exception. Derived classes are free to write their
   * derivation if necessary.
   *
   * @deprecated Use actions defined via {@code solr.xml} instead.
   */
  @Deprecated
  protected void handleCustomAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Unsupported operation: " + req.getParams().get(ACTION));
  }

  public static Map<String, String> paramToProp =
      Map.ofEntries(
          Map.entry(CoreAdminParams.CONFIG, CoreDescriptor.CORE_CONFIG),
          Map.entry(CoreAdminParams.SCHEMA, CoreDescriptor.CORE_SCHEMA),
          Map.entry(CoreAdminParams.DATA_DIR, CoreDescriptor.CORE_DATADIR),
          Map.entry(CoreAdminParams.ULOG_DIR, CoreDescriptor.CORE_ULOGDIR),
          Map.entry(CoreAdminParams.CONFIGSET, CoreDescriptor.CORE_CONFIGSET),
          Map.entry(CoreAdminParams.LOAD_ON_STARTUP, CoreDescriptor.CORE_LOADONSTARTUP),
          Map.entry(CoreAdminParams.TRANSIENT, CoreDescriptor.CORE_TRANSIENT),
          Map.entry(CoreAdminParams.SHARD, CoreDescriptor.CORE_SHARD),
          Map.entry(CoreAdminParams.COLLECTION, CoreDescriptor.CORE_COLLECTION),
          Map.entry(CoreAdminParams.CORE_NODE_NAME, CoreDescriptor.CORE_NODE_NAME),
          Map.entry(CoreAdminParams.REPLICA_TYPE, CloudDescriptor.REPLICA_TYPE));

  private static Map<String, CoreAdminOp> initializeOpMap() {
    Map<String, CoreAdminOp> opMap = new HashMap<>();
    for (CoreAdminOperation op : CoreAdminOperation.values()) {
      opMap.put(op.action.toString().toLowerCase(Locale.ROOT), op);
    }
    return opMap;
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
    return "Manage Multiple Solr Cores";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext ctx) {
    String action = ctx.getParams().get(CoreAdminParams.ACTION);
    if (action == null) return CORE_READ_PERM;
    CoreAdminParams.CoreAdminAction coreAction = CoreAdminParams.CoreAdminAction.get(action);
    if (coreAction == null) return CORE_READ_PERM;
    return coreAction.isRead ? CORE_READ_PERM : CORE_EDIT_PERM;
  }

  /** Method to ensure shutting down of the ThreadPool Executor. */
  public void shutdown() {
    coreAdminAsyncTracker.shutdown();
  }

  public static class CallInfo {
    public final CoreAdminHandler handler;
    public final SolrQueryRequest req;
    public final SolrQueryResponse rsp;
    public final CoreAdminOp op;

    CallInfo(
        CoreAdminHandler handler, SolrQueryRequest req, SolrQueryResponse rsp, CoreAdminOp op) {
      this.handler = handler;
      this.req = req;
      this.rsp = rsp;
      this.op = op;
    }

    void call() throws Exception {
      op.execute(this);
    }
  }

  @Override
  public Collection<Api> getApis() {
    final List<Api> apis = new ArrayList<>();
    apis.addAll(AnnotatedApi.getApis(new RejoinLeaderElectionAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new OverseerOperationAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new SplitCoreAPI(this)));
    // Internal APIs
    apis.addAll(AnnotatedApi.getApis(new RequestCoreRecoveryAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new PrepareCoreRecoveryAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new RequestApplyCoreUpdatesAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new RequestSyncShardAPI(this)));
    apis.addAll(AnnotatedApi.getApis(new RequestBufferUpdatesAPI(this)));

    return apis;
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(
        CoreSnapshot.class,
        CoreStatus.class,
        InstallCoreData.class,
        CreateCore.class,
        CreateCoreBackup.class,
        RestoreCore.class,
        ReloadCore.class,
        UnloadCore.class,
        SwapCores.class,
        RenameCore.class,
        MergeIndexes.class,
        GetNodeCommandStatus.class);
  }

  public interface CoreAdminOp {

    default boolean isExpensive() {
      return false;
    }

    /**
     * @param it request/response object
     *     <p>If the request is invalid throw a SolrException with
     *     SolrException.ErrorCode.BAD_REQUEST ( 400 ) If the execution of the command fails throw a
     *     SolrException with SolrException.ErrorCode.SERVER_ERROR ( 500 )
     *     <p>Any non-SolrException's are wrapped at a higher level as a SolrException with
     *     SolrException.ErrorCode.SERVER_ERROR.
     */
    void execute(CallInfo it) throws Exception;
  }

  public static class CoreAdminAsyncTracker {
    /**
     * Max number of requests we track in the Caffeine cache. This limit is super high on purpose,
     * we're not supposed to hit it. This is just a protection to grow in memory too much when
     * receiving an abusive number of admin requests.
     */
    private static final int MAX_TRACKED_REQUESTS =
        EnvUtils.getPropertyAsInteger("solr.admin.async.max", 10_000);

    public static final String RUNNING = "running";
    public static final String COMPLETED = "completed";
    public static final String FAILED = "failed";

    private final Cache<String, TaskObject> requestStatusCache; // key by ID

    // Executor for all standard tasks (the ones that are not flagged as expensive)
    // We always keep 50 live threads
    private ExecutorService standardExecutor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            50, new SolrNamedThreadFactory("parallelCoreAdminAPIBaseExecutor"));

    // Executor for expensive tasks
    // We keep the number of max threads very low to have throttling for expensive tasks
    private ExecutorService expensiveExecutor =
        ExecutorUtil.newMDCAwareCachedThreadPool(
            5,
            Integer.MAX_VALUE,
            new SolrNamedThreadFactory("parallelCoreAdminAPIExpensiveExecutor"));

    public CoreAdminAsyncTracker() {
      this(
          Ticker.systemTicker(),
          TimeUnit.MINUTES.toNanos(
              EnvUtils.getPropertyAsLong("solr.admin.async.timeout.minutes", 60L)),
          TimeUnit.MINUTES.toNanos(
              EnvUtils.getPropertyAsLong("solr.admin.async.timeout.completed.minutes", 5L)));
    }

    /**
     * @param runningTimeoutNanos The time-to-keep for tasks in the RUNNING state.
     * @param completedTimeoutNanos The time-to-keep for tasks in the COMPLETED or FAILED state
     *     after the status was polled.
     */
    CoreAdminAsyncTracker(Ticker ticker, long runningTimeoutNanos, long completedTimeoutNanos) {

      TaskExpiry expiry = new TaskExpiry(runningTimeoutNanos, completedTimeoutNanos);
      requestStatusCache =
          Caffeine.newBuilder()
              .ticker(ticker)
              .maximumSize(MAX_TRACKED_REQUESTS)
              .expireAfter(expiry)
              .build();
    }

    public void shutdown() {
      ExecutorUtil.shutdownAndAwaitTermination(standardExecutor);
      ExecutorUtil.shutdownAndAwaitTermination(expensiveExecutor);
    }

    public TaskObject getAsyncRequestForStatus(String key) {
      TaskObject task = requestStatusCache.getIfPresent(key);

      if (task != null && !RUNNING.equals(task.status) && !task.polledAfterCompletion) {
        task.polledAfterCompletion = true;
        // At the first time we retrieve the status of a completed request, do a second lookup in
        // the cache. This is necessary to update the TTL of this request in the cache.
        // Unfortunately, we can't force the expiration time to be refreshed without a lookup.
        requestStatusCache.getIfPresent(key);
      }

      return task;
    }

    public void submitAsyncTask(TaskObject taskObject) throws SolrException {
      addTask(taskObject);

      Runnable command =
          () -> {
            boolean exceptionCaught = false;
            try {
              final SolrQueryResponse response = taskObject.task.call();
              taskObject.setRspObject(response);
              taskObject.setOperationRspObject(response);
            } catch (Exception e) {
              exceptionCaught = true;
              taskObject.setRspObjectFromException(e);
            } finally {
              finishTask(taskObject, !exceptionCaught);
            }
          };

      try {
        MDC.put("CoreAdminHandler.asyncId", taskObject.taskId);
        MDC.put("CoreAdminHandler.action", taskObject.action);
        if (taskObject.expensive) {
          expensiveExecutor.execute(command);
        } else {
          standardExecutor.execute(command);
        }
      } finally {
        MDC.remove("CoreAdminHandler.asyncId");
        MDC.remove("CoreAdminHandler.action");
      }
    }

    private void addTask(TaskObject taskObject) {
      // Ensure task ID is not already in use
      TaskObject taskInCache =
          requestStatusCache.get(
              taskObject.taskId,
              n -> {
                taskObject.status = RUNNING;
                return taskObject;
              });

      // If we get a different task instance, it means one was already in the cache with the
      // same name. Just reject the new one.
      if (taskInCache != taskObject) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST, "Duplicate request with the same requestid found.");
      }
    }

    private void finishTask(TaskObject taskObject, boolean successful) {
      taskObject.status = successful ? COMPLETED : FAILED;
    }

    /**
     * Helper class to manage the tasks to be tracked. This contains the taskId, request and the
     * response (if available).
     */
    public static class TaskObject {
      final String taskId;
      final String action;
      final boolean expensive;
      final Callable<SolrQueryResponse> task;
      public String rspInfo;
      public Object operationRspInfo;
      private volatile String status;

      /**
       * Flag set to true once the task is complete (can be in error) and the status was polled
       * already once. Once set, the time we keep the task status is shortened.
       */
      private volatile boolean polledAfterCompletion;

      public TaskObject(
          String taskId, String action, boolean expensive, Callable<SolrQueryResponse> task) {
        this.taskId = taskId;
        this.action = action;
        this.expensive = expensive;
        this.task = task;
      }

      public String getRspObject() {
        return rspInfo;
      }

      public void setRspObject(SolrQueryResponse rspObject) {
        this.rspInfo = rspObject.getToLogAsString("TaskId: " + this.taskId);
      }

      public void setRspObjectFromException(Exception e) {
        this.rspInfo = e.getMessage();
      }

      public Object getOperationRspObject() {
        return operationRspInfo;
      }

      public void setOperationRspObject(SolrQueryResponse rspObject) {
        this.operationRspInfo = rspObject.getResponse();
      }

      public String getStatus() {
        return status;
      }
    }

    /**
     * Expiration policy for Caffeine cache. Depending on whether the status of a completed task was
     * already retrieved, we return {@link #runningTimeoutNanos} or {@link #completedTimeoutNanos}.
     */
    private static class TaskExpiry implements Expiry<String, TaskObject> {

      private final long runningTimeoutNanos;
      private final long completedTimeoutNanos;

      private TaskExpiry(long runningTimeoutNanos, long completedTimeoutNanos) {
        this.runningTimeoutNanos = runningTimeoutNanos;
        this.completedTimeoutNanos = completedTimeoutNanos;
      }

      @Override
      public long expireAfterCreate(String key, TaskObject task, long currentTime) {
        return runningTimeoutNanos;
      }

      @Override
      public long expireAfterUpdate(
          String key, TaskObject task, long currentTime, long currentDuration) {
        return task.polledAfterCompletion ? completedTimeoutNanos : runningTimeoutNanos;
      }

      @Override
      public long expireAfterRead(
          String key, TaskObject task, long currentTime, long currentDuration) {
        return task.polledAfterCompletion ? completedTimeoutNanos : runningTimeoutNanos;
      }
    }
  }
}

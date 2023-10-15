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
package org.apache.solr.handler.component;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.FAILURE;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CommonParams.STATUS;

import com.codahale.metrics.Counter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.pkg.PackageAPI;
import org.apache.solr.pkg.PackageListeners;
import org.apache.solr.pkg.SolrPackageLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.CursorMark;
import org.apache.solr.search.SolrQueryTimeoutImpl;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.facet.FacetModule;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Refer SOLR-281 */
public class SearchHandler extends RequestHandlerBase
    implements SolrCoreAware, PluginInfoInitialized, PermissionNameProvider {
  static final String INIT_COMPONENTS = "components";
  static final String INIT_FIRST_COMPONENTS = "first-components";
  static final String INIT_LAST_COMPONENTS = "last-components";

  protected static final String SHARD_HANDLER_SUFFIX = "[shard]";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * A counter to ensure that no RID is equal, even if they fall in the same millisecond
   *
   * @deprecated this was replaced by the auto-generated trace ids
   */
  @Deprecated(since = "9.4")
  private static final AtomicLong ridCounter = new AtomicLong();

  /**
   * An opt-out flag to prevent the addition of {@link CommonParams#REQUEST_ID} tracing on
   * distributed queries
   *
   * <p>Defaults to 'false' if not specified.
   *
   * @see CommonParams#DISABLE_REQUEST_ID
   * @deprecated this was replaced by the auto-generated trace ids
   */
  @Deprecated(since = "9.4")
  private static final boolean DISABLE_REQUEST_ID_DEFAULT =
      Boolean.getBoolean("solr.disableRequestId");

  private HandlerMetrics metricsShard = HandlerMetrics.NO_OP;
  private final Map<String, Counter> shardPurposes = new ConcurrentHashMap<>();

  protected volatile List<SearchComponent> components;
  private ShardHandlerFactory shardHandlerFactory;
  private PluginInfo shfInfo;
  private SolrCore core;

  protected List<String> getDefaultComponents() {
    ArrayList<String> names = new ArrayList<>(8);
    names.add(QueryComponent.COMPONENT_NAME);
    names.add(FacetComponent.COMPONENT_NAME);
    names.add(FacetModule.COMPONENT_NAME);
    names.add(MoreLikeThisComponent.COMPONENT_NAME);
    names.add(HighlightComponent.COMPONENT_NAME);
    names.add(StatsComponent.COMPONENT_NAME);
    names.add(DebugComponent.COMPONENT_NAME);
    names.add(ExpandComponent.COMPONENT_NAME);
    names.add(TermsComponent.COMPONENT_NAME);

    return names;
  }

  @Override
  public void init(PluginInfo info) {
    init(info.initArgs);
    for (PluginInfo child : info.children) {
      if ("shardHandlerFactory".equals(child.type)) {
        this.shfInfo = child;
        break;
      }
    }
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);
    metricsShard =
        new HandlerMetrics( // will register various metrics in the context
            solrMetricsContext, getCategory().toString(), scope + SHARD_HANDLER_SUFFIX);
    solrMetricsContext.gauge(
        new MetricsMap(map -> shardPurposes.forEach((k, v) -> map.putNoEx(k, v.getCount()))),
        true,
        "purposes",
        getCategory().toString(),
        scope + SHARD_HANDLER_SUFFIX);
  }

  @Override
  public HandlerMetrics getMetricsForThisRequest(SolrQueryRequest req) {
    return req.getParams().getBool(ShardParams.IS_SHARD, false) ? this.metricsShard : this.metrics;
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  /**
   * Initialize the components based on name. Note, if using <code>INIT_FIRST_COMPONENTS</code> or
   * <code>INIT_LAST_COMPONENTS</code>, then the {@link DebugComponent} will always occur last. If
   * this is not desired, then one must explicitly declare all components using the <code>
   * INIT_COMPONENTS</code> syntax.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void inform(SolrCore core) {
    this.core = core;
    List<String> c = (List<String>) initArgs.get(INIT_COMPONENTS);
    Set<String> missing = new HashSet<>(core.getSearchComponents().checkContains(c));
    List<String> first = (List<String>) initArgs.get(INIT_FIRST_COMPONENTS);
    missing.addAll(core.getSearchComponents().checkContains(first));
    List<String> last = (List<String>) initArgs.get(INIT_LAST_COMPONENTS);
    missing.addAll(core.getSearchComponents().checkContains(last));
    if (!missing.isEmpty())
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Missing SearchComponents named : " + missing);
    if (c != null && (first != null || last != null))
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "First/Last components only valid if you do not declare 'components'");

    if (shfInfo == null) {
      shardHandlerFactory = core.getCoreContainer().getShardHandlerFactory();
    } else {
      shardHandlerFactory = core.createInitInstance(shfInfo, ShardHandlerFactory.class, null, null);
      core.addCloseHook(
          new CloseHook() {
            @Override
            public void preClose(SolrCore core) {
              shardHandlerFactory.close();
            }
          });
      shardHandlerFactory.setSecurityBuilder(
          core.getCoreContainer().getPkiAuthenticationSecurityBuilder());
    }

    if (core.getCoreContainer().isZooKeeperAware()) {
      core.getPackageListeners()
          .addListener(
              new PackageListeners.Listener() {
                @Override
                public String packageName() {
                  return null;
                }

                @Override
                public Map<String, PackageAPI.PkgVersion> packageDetails() {
                  return Collections.emptyMap();
                }

                @Override
                public void changed(SolrPackageLoader.SolrPackage pkg, Ctx ctx) {
                  // we could optimize this by listening to only relevant packages,
                  // but it is not worth optimizing as these are lightweight objects
                  components = null;
                }
              });
    }
  }

  @SuppressWarnings({"unchecked"})
  private void initComponents() {
    Object declaredComponents = initArgs.get(INIT_COMPONENTS);
    List<String> first = (List<String>) initArgs.get(INIT_FIRST_COMPONENTS);
    List<String> last = (List<String>) initArgs.get(INIT_LAST_COMPONENTS);

    List<String> list = null;
    boolean makeDebugLast = true;
    if (declaredComponents == null) {
      // Use the default component list
      list = getDefaultComponents();

      if (first != null) {
        List<String> clist = first;
        clist.addAll(list);
        list = clist;
      }

      if (last != null) {
        list.addAll(last);
      }
    } else {
      list = (List<String>) declaredComponents;
      if (first != null || last != null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "First/Last components only valid if you do not declare 'components'");
      }
      makeDebugLast = false;
    }

    // Build the component list
    List<SearchComponent> components = new ArrayList<>(list.size());
    DebugComponent dbgCmp = null;
    for (String c : list) {
      SearchComponent comp = core.getSearchComponent(c);
      if (comp instanceof DebugComponent && makeDebugLast == true) {
        dbgCmp = (DebugComponent) comp;
      } else {
        components.add(comp);
        log.debug("Adding  component:{}", comp);
      }
    }
    if (makeDebugLast == true && dbgCmp != null) {
      components.add(dbgCmp);
      log.debug("Adding  debug component:{}", dbgCmp);
    }
    this.components = components;
  }

  public List<SearchComponent> getComponents() {
    List<SearchComponent> result = components; // volatile read
    if (result == null) {
      synchronized (this) {
        if (components == null) {
          initComponents();
        }
        result = components;
      }
    }
    return result;
  }

  private boolean isDistrib(SolrQueryRequest req) {
    boolean isZkAware = req.getCoreContainer().isZooKeeperAware();
    boolean isDistrib = req.getParams().getBool(DISTRIB, isZkAware);
    if (!isDistrib) {
      // for back compat, a shards param with URLs like localhost:8983/solr will mean that this
      // search is distributed.
      final String shards = req.getParams().get(ShardParams.SHARDS);
      isDistrib = ((shards != null) && (shards.indexOf('/') > 0));
    }
    return isDistrib;
  }

  public ShardHandler getAndPrepShardHandler(SolrQueryRequest req, ResponseBuilder rb) {
    ShardHandler shardHandler = null;

    CoreContainer cc = req.getCoreContainer();
    boolean isZkAware = cc.isZooKeeperAware();

    if (rb.isDistrib) {
      shardHandler = shardHandlerFactory.getShardHandler();
      shardHandler.prepDistributed(rb);
      if (!rb.isDistrib) {
        // request is not distributed after all and so the shard handler is not needed
        shardHandler = null;
      }
    }

    if (isZkAware) {
      String shardsTolerant = req.getParams().get(ShardParams.SHARDS_TOLERANT);
      boolean requireZkConnected =
          shardsTolerant != null && shardsTolerant.equals(ShardParams.REQUIRE_ZK_CONNECTED);
      ZkController zkController = cc.getZkController();
      boolean zkConnected =
          zkController != null
              && !zkController.getZkClient().getConnectionManager().isLikelyExpired();
      if (requireZkConnected && false == zkConnected) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ZooKeeper is not connected");
      } else {
        NamedList<Object> headers = rb.rsp.getResponseHeader();
        if (headers != null) {
          headers.add("zkConnected", zkConnected);
        }
      }
    }

    return shardHandler;
  }

  /**
   * Override this method if you require a custom {@link ResponseBuilder} e.g. for use by a custom
   * {@link SearchComponent}.
   */
  protected ResponseBuilder newResponseBuilder(
      SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
    return new ResponseBuilder(req, rsp, components);
  }

  /**
   * Check if {@link SolrRequestType#QUERY} circuit breakers are tripped. Override this method in
   * sub classes that do not want to check circuit breakers.
   *
   * @return true if circuit breakers are tripped, false otherwise.
   */
  protected boolean checkCircuitBreakers(
      SolrQueryRequest req, SolrQueryResponse rsp, ResponseBuilder rb) {
    if (isInternalShardRequest(req)) {
      if (log.isTraceEnabled()) {
        log.trace("Internal request, skipping circuit breaker check");
      }
      return false;
    }
    final RTimerTree timer = rb.isDebug() ? req.getRequestTimer() : null;

    final CircuitBreakerRegistry circuitBreakerRegistry = req.getCore().getCircuitBreakerRegistry();
    if (circuitBreakerRegistry.isEnabled(SolrRequestType.QUERY)) {
      List<CircuitBreaker> trippedCircuitBreakers;

      if (timer != null) {
        RTimerTree subt = timer.sub("circuitbreaker");
        rb.setTimer(subt);

        trippedCircuitBreakers = circuitBreakerRegistry.checkTripped(SolrRequestType.QUERY);

        rb.getTimer().stop();
      } else {
        trippedCircuitBreakers = circuitBreakerRegistry.checkTripped(SolrRequestType.QUERY);
      }

      if (trippedCircuitBreakers != null) {
        String errorMessage = CircuitBreakerRegistry.toErrorMessage(trippedCircuitBreakers);
        rsp.add(STATUS, FAILURE);
        rsp.setException(
            new SolrException(
                CircuitBreaker.getErrorCode(trippedCircuitBreakers),
                "Circuit Breakers tripped " + errorMessage));
        return true;
      }
    }
    return false;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (req.getParams().getBool(ShardParams.IS_SHARD, false)) {
      int purpose = req.getParams().getInt(ShardParams.SHARDS_PURPOSE, 0);
      SolrPluginUtils.forEachRequestPurpose(
          purpose, n -> shardPurposes.computeIfAbsent(n, name -> new Counter()).inc());
    }

    List<SearchComponent> components = getComponents();
    ResponseBuilder rb = newResponseBuilder(req, rsp, components);
    if (rb.requestInfo != null) {
      rb.requestInfo.setResponseBuilder(rb);
    }

    rb.isDistrib = isDistrib(req);
    tagRequestWithRequestId(rb);

    boolean dbg = req.getParams().getBool(CommonParams.DEBUG_QUERY, false);
    rb.setDebug(dbg);
    if (dbg == false) { // if it's true, we are doing everything anyway.
      SolrPluginUtils.getDebugInterests(req.getParams().getParams(CommonParams.DEBUG), rb);
    }

    final RTimerTree timer = rb.isDebug() ? req.getRequestTimer() : null;

    if (checkCircuitBreakers(req, rsp, rb)) {
      return; // Circuit breaker tripped, return immediately
    }

    // creates a ShardHandler object only if it's needed
    final ShardHandler shardHandler1 = getAndPrepShardHandler(req, rb);

    if (timer == null) {
      // non-debugging prepare phase
      for (SearchComponent c : components) {
        c.prepare(rb);
      }
    } else {
      // debugging prepare phase
      RTimerTree subt = timer.sub("prepare");
      for (SearchComponent c : components) {
        rb.setTimer(subt.sub(c.getName()));
        c.prepare(rb);
        rb.getTimer().stop();
      }
      subt.stop();
    }

    { // Once all of our components have been prepared, check if this request involves a SortSpec.
      // If it does, and if our request includes a cursorMark param, then parse & init the
      // CursorMark state (This must happen after the prepare() of all components, because any
      // component may have modified the SortSpec)
      final SortSpec spec = rb.getSortSpec();
      final String cursorStr = rb.req.getParams().get(CursorMarkParams.CURSOR_MARK_PARAM);
      if (null != spec && null != cursorStr) {
        final CursorMark cursorMark = new CursorMark(rb.req.getSchema(), spec);
        cursorMark.parseSerializedTotem(cursorStr);
        rb.setCursorMark(cursorMark);
      }
    }

    if (!rb.isDistrib) {
      // a normal non-distributed request

      SolrQueryTimeoutImpl.set(req);
      try {
        // The semantics of debugging vs not debugging are different enough that
        // it makes sense to have two control loops
        if (!rb.isDebug()) {
          // Process
          for (SearchComponent c : components) {
            c.process(rb);
          }
        } else {
          // Process
          RTimerTree subt = timer.sub("process");
          for (SearchComponent c : components) {
            rb.setTimer(subt.sub(c.getName()));
            c.process(rb);
            rb.getTimer().stop();
          }
          subt.stop();

          // add the timing info
          if (rb.isDebugTimings()) {
            rb.addDebugInfo("timing", timer.asNamedList());
          }
        }
      } catch (ExitableDirectoryReader.ExitingReaderException ex) {
        log.warn("Query: {}; ", req.getParamString(), ex);
        if (rb.rsp.getResponse() == null) {
          rb.rsp.addResponse(new SolrDocumentList());

          // If a cursorMark was passed, and we didn't progress, set
          // the nextCursorMark to the same position
          String cursorStr = rb.req.getParams().get(CursorMarkParams.CURSOR_MARK_PARAM);
          if (null != cursorStr) {
            rb.rsp.add(CursorMarkParams.CURSOR_MARK_NEXT, cursorStr);
          }
        }
        if (rb.isDebug()) {
          NamedList<Object> debug = new NamedList<>();
          debug.add("explain", new NamedList<>());
          rb.rsp.add("debug", debug);
        }
        rb.rsp
            .getResponseHeader()
            .asShallowMap()
            .put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
      } finally {
        SolrQueryTimeoutImpl.reset();
      }
    } else {
      // a distributed request

      if (rb.outgoing == null) {
        rb.outgoing = new ArrayList<>();
      }
      rb.finished = new ArrayList<>();

      int nextStage = 0;
      do {
        rb.stage = nextStage;
        nextStage = ResponseBuilder.STAGE_DONE;

        // call all components
        for (SearchComponent c : components) {
          // the next stage is the minimum of what all components report
          nextStage = Math.min(nextStage, c.distributedProcess(rb));
        }

        // check the outgoing queue and send requests
        while (rb.outgoing.size() > 0) {

          // submit all current request tasks at once
          while (rb.outgoing.size() > 0) {
            ShardRequest sreq = rb.outgoing.remove(0);
            sreq.actualShards = sreq.shards;
            if (sreq.actualShards == ShardRequest.ALL_SHARDS) {
              sreq.actualShards = rb.shards;
            }
            // presume we'll get a response from each shard we send to
            sreq.responses = new ArrayList<>(sreq.actualShards.length);

            // TODO: map from shard to address[]
            for (String shard : sreq.actualShards) {
              ModifiableSolrParams params = new ModifiableSolrParams(sreq.params);
              params.setShardAttributesToParams(sreq.purpose);

              // Distributed request -- need to send queryID as a part of the distributed request
              params.setNonNull(ShardParams.QUERY_ID, rb.queryID);
              if (rb.requestInfo != null) {
                // we could try and detect when this is needed, but it could be tricky
                params.set("NOW", Long.toString(rb.requestInfo.getNOW().getTime()));
              }
              String shardQt = params.get(ShardParams.SHARDS_QT);
              if (shardQt != null) {
                params.set(CommonParams.QT, shardQt);
              } else {
                // for distributed queries that don't include shards.qt, use the original path
                // as the default but operators need to update their luceneMatchVersion to enable
                // this behavior since it did not work this way prior to 5.1
                String reqPath = (String) req.getContext().get(PATH);
                if (!"/select".equals(reqPath)) {
                  params.set(CommonParams.QT, reqPath);
                } // else if path is /select, then the qt gets passed thru if set
              }
              shardHandler1.submit(sreq, shard, params);
            }
          }

          // now wait for replies, but if anyone puts more requests on
          // the outgoing queue, send them out immediately (by exiting
          // this loop)
          boolean tolerant = ShardParams.getShardsTolerantAsBool(rb.req.getParams());
          while (rb.outgoing.size() == 0) {
            ShardResponse srsp =
                tolerant
                    ? shardHandler1.takeCompletedIncludingErrors()
                    : shardHandler1.takeCompletedOrError();
            if (srsp == null) break; // no more requests to wait for

            // Was there an exception?
            if (srsp.getException() != null) {
              // If things are not tolerant, abort everything and rethrow
              if (!tolerant) {
                shardHandler1.cancelAll();
                if (srsp.getException() instanceof SolrException) {
                  throw (SolrException) srsp.getException();
                } else {
                  throw new SolrException(
                      SolrException.ErrorCode.SERVER_ERROR, srsp.getException());
                }
              } else {
                rsp.getResponseHeader()
                    .asShallowMap()
                    .put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
              }
            }

            rb.finished.add(srsp.getShardRequest());

            // let the components see the responses to the request
            for (SearchComponent c : components) {
              c.handleResponses(rb, srsp.getShardRequest());
            }
          }
        }

        for (SearchComponent c : components) {
          c.finishStage(rb);
        }

        // we are done when the next stage is MAX_VALUE
      } while (nextStage != Integer.MAX_VALUE);
    }

    // SOLR-5550: still provide shards.info if requested even for a short circuited distrib request
    if (!rb.isDistrib
        && req.getParams().getBool(ShardParams.SHARDS_INFO, false)
        && rb.shortCircuitedURL != null) {
      NamedList<Object> shardInfo = new SimpleOrderedMap<>();
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
      if (rsp.getException() != null) {
        Throwable cause = rsp.getException();
        if (cause instanceof SolrServerException) {
          cause = ((SolrServerException) cause).getRootCause();
        } else {
          if (cause.getCause() != null) {
            cause = cause.getCause();
          }
        }
        nl.add("error", cause.toString());
        if (!core.getCoreContainer().hideStackTrace()) {
          StringWriter trace = new StringWriter();
          cause.printStackTrace(new PrintWriter(trace));
          nl.add("trace", trace.toString());
        }
      } else if (rb.getResults() != null) {
        nl.add("numFound", rb.getResults().docList.matches());
        nl.add(
            "numFoundExact",
            rb.getResults().docList.hitCountRelation() == TotalHits.Relation.EQUAL_TO);
        nl.add("maxScore", rb.getResults().docList.maxScore());
      }
      nl.add("shardAddress", rb.shortCircuitedURL);
      nl.add("time", req.getRequestTimer().getTime()); // elapsed time of this request so far

      int pos = rb.shortCircuitedURL.indexOf("://");
      String shardInfoName =
          pos != -1 ? rb.shortCircuitedURL.substring(pos + 3) : rb.shortCircuitedURL;
      shardInfo.add(shardInfoName, nl);
      rsp.getValues().add(ShardParams.SHARDS_INFO, shardInfo);
    }
  }

  private void tagRequestWithRequestId(ResponseBuilder rb) {
    final boolean ridTaggingDisabled =
        rb.req.getParams().getBool(CommonParams.DISABLE_REQUEST_ID, DISABLE_REQUEST_ID_DEFAULT);
    if (!ridTaggingDisabled) {
      String rid = getOrGenerateRequestId(rb.req);

      // NOTE: SearchHandler explicitly never clears/removes this MDC value...
      // We want it to live for the entire request, beyond the scope of SearchHandler's processing,
      // and trust SolrDispatchFilter to clean it up at the end of the request.
      //
      // Examples:
      // - ERROR logging of Exceptions propogated up to our base class
      // - SolrCore.RequestLog
      // - ERRORs that may be logged during response writing
      MDC.put(CommonParams.REQUEST_ID, rid);

      if (StrUtils.isBlank(rb.req.getParams().get(CommonParams.REQUEST_ID))) {
        ModifiableSolrParams params = new ModifiableSolrParams(rb.req.getParams());
        params.add(CommonParams.REQUEST_ID, rid); // add rid to the request so that shards see it
        rb.req.setParams(params);
      }
      if (rb.isDistrib) {
        rb.rsp.addToLog(CommonParams.REQUEST_ID, rid); // to see it in the logs of the landing core
      }
    }
  }

  /**
   * Returns a String to use as an identifier for this request.
   *
   * <p>If the provided {@link SolrQueryRequest} contains a non-blank {@link
   * CommonParams#REQUEST_ID} param value this is used. This is especially useful for users who
   * deploy Solr as one component in a larger ecosystem, and want to use an external ID utilized by
   * other components as well. If no {@link CommonParams#REQUEST_ID} value is present, one is
   * generated from scratch for the request.
   *
   * <p>Callers are responsible for storing the returned value in the {@link SolrQueryRequest}
   * object if they want to ensure that ID generation is not redone on subsequent calls.
   */
  public static String getOrGenerateRequestId(SolrQueryRequest req) {
    String rid = req.getParams().get(CommonParams.REQUEST_ID);
    if (StrUtils.isNotBlank(rid)) {
      return rid;
    }
    String traceId = MDCLoggingContext.getTraceId();
    if (StrUtils.isNotBlank(traceId)) {
      return traceId;
    }
    return generateRid(req);
  }

  private static String generateRid(SolrQueryRequest req) {
    String hostName = req.getCoreContainer().getHostName();
    return hostName + "-" + ridCounter.getAndIncrement();
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    StringBuilder sb = new StringBuilder();
    sb.append("Search using components: ");
    if (components != null) {
      for (SearchComponent c : components) {
        sb.append(c.getName());
        sb.append(",");
      }
    }
    return sb.toString();
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}

// TODO: generalize how a comm component can fit into search component framework
// TODO: statics should be per-core singletons

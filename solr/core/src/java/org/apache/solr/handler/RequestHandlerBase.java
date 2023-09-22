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
package org.apache.solr.handler;

import static org.apache.solr.core.RequestParams.USEPARAM;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.ApiSupport;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.MetricsConfig;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrDelegateRegistryMetricsContext;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for all request handlers. */
public abstract class RequestHandlerBase
    implements SolrRequestHandler,
        SolrInfoBean,
        NestedRequestHandler,
        ApiSupport,
        PermissionNameProvider {

  protected NamedList<?> initArgs = null;
  protected SolrParams defaults;
  protected SolrParams appends;
  protected SolrParams invariants;
  protected boolean httpCaching = true;
  protected boolean aggregateNodeLevelMetricsEnabled = false;

  protected SolrMetricsContext solrMetricsContext;
  protected HandlerMetrics metrics = HandlerMetrics.NO_OP;
  private final long handlerStart;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private PluginInfo pluginInfo;

  @SuppressForbidden(reason = "Need currentTimeMillis, used only for stats output")
  public RequestHandlerBase() {
    handlerStart = System.currentTimeMillis();
  }

  /**
   * Initializes the {@link org.apache.solr.request.SolrRequestHandler} by creating three {@link
   * org.apache.solr.common.params.SolrParams} named.
   *
   * <table style="border: 1px solid">
   * <caption>table of parameters</caption>
   * <tr><th>Name</th><th>Description</th></tr>
   * <tr><td>defaults</td><td>Contains all of the named arguments contained within the list element named "defaults".</td></tr>
   * <tr><td>appends</td><td>Contains all of the named arguments contained within the list element named "appends".</td></tr>
   * <tr><td>invariants</td><td>Contains all of the named arguments contained within the list element named "invariants".</td></tr>
   * </table>
   *
   * <p>Example:
   *
   * <pre>
   * &lt;lst name="defaults"&gt;
   * &lt;str name="echoParams"&gt;explicit&lt;/str&gt;
   * &lt;str name="qf"&gt;text^0.5 features^1.0 name^1.2 sku^1.5 id^10.0&lt;/str&gt;
   * &lt;str name="mm"&gt;2&lt;-1 5&lt;-2 6&lt;90%&lt;/str&gt;
   * &lt;str name="bq"&gt;incubationdate_dt:[* TO NOW/DAY-1MONTH]^2.2&lt;/str&gt;
   * &lt;/lst&gt;
   * &lt;lst name="appends"&gt;
   * &lt;str name="fq"&gt;inStock:true&lt;/str&gt;
   * &lt;/lst&gt;
   *
   * &lt;lst name="invariants"&gt;
   * &lt;str name="facet.field"&gt;cat&lt;/str&gt;
   * &lt;str name="facet.field"&gt;manu_exact&lt;/str&gt;
   * &lt;str name="facet.query"&gt;price:[* TO 500]&lt;/str&gt;
   * &lt;str name="facet.query"&gt;price:[500 TO *]&lt;/str&gt;
   * &lt;/lst&gt;
   * </pre>
   *
   * @param args The {@link org.apache.solr.common.util.NamedList} to initialize from
   * @see #handleRequest(org.apache.solr.request.SolrQueryRequest,
   *     org.apache.solr.response.SolrQueryResponse)
   * @see #handleRequestBody(org.apache.solr.request.SolrQueryRequest,
   *     org.apache.solr.response.SolrQueryResponse)
   * @see org.apache.solr.util.SolrPluginUtils#setDefaults(org.apache.solr.request.SolrQueryRequest,
   *     org.apache.solr.common.params.SolrParams, org.apache.solr.common.params.SolrParams,
   *     org.apache.solr.common.params.SolrParams)
   * @see NamedList#toSolrParams()
   *     <p>See also the example solrconfig.xml located in the Solr codebase (example/solr/conf).
   */
  @Override
  public void init(NamedList<?> args) {
    initArgs = args;

    if (args != null) {
      defaults = getSolrParamsFromNamedList(args, "defaults");
      appends = getSolrParamsFromNamedList(args, "appends");
      invariants = getSolrParamsFromNamedList(args, "invariants");
    }

    if (initArgs != null) {
      Object caching = initArgs.get("httpCaching");
      httpCaching = caching != null ? Boolean.parseBoolean(caching.toString()) : true;
      Boolean aggregateNodeLevelMetricsEnabled =
          initArgs.getBooleanArg("aggregateNodeLevelMetricsEnabled");
      if (aggregateNodeLevelMetricsEnabled != null) {
        this.aggregateNodeLevelMetricsEnabled = aggregateNodeLevelMetricsEnabled;
      }
    }
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    if (aggregateNodeLevelMetricsEnabled) {
      this.solrMetricsContext =
          new SolrDelegateRegistryMetricsContext(
              parentContext.getMetricManager(),
              parentContext.getRegistryName(),
              SolrMetricProducer.getUniqueMetricTag(this, parentContext.getTag()),
              SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));
    } else {
      this.solrMetricsContext = parentContext.getChildContext(this);
    }
    metrics = new HandlerMetrics(solrMetricsContext, getCategory().toString(), scope);
    solrMetricsContext.gauge(
        () -> handlerStart, true, "handlerStart", getCategory().toString(), scope);
  }

  /** Metrics for this handler. */
  public static class HandlerMetrics {
    public static final HandlerMetrics NO_OP =
        new HandlerMetrics(
            new SolrMetricsContext(
                new SolrMetricManager(
                    null, new MetricsConfig.MetricsConfigBuilder().setEnabled(false).build()),
                "NO_OP",
                "NO_OP"));

    public final Meter numErrors;
    public final Meter numServerErrors;
    public final Meter numClientErrors;
    public final Meter numTimeouts;
    public final Counter requests;
    public final Timer requestTimes;
    public final Counter totalTime;

    public HandlerMetrics(SolrMetricsContext solrMetricsContext, String... metricPath) {
      numErrors = solrMetricsContext.meter("errors", metricPath);
      numServerErrors = solrMetricsContext.meter("serverErrors", metricPath);
      numClientErrors = solrMetricsContext.meter("clientErrors", metricPath);
      numTimeouts = solrMetricsContext.meter("timeouts", metricPath);
      requests = solrMetricsContext.counter("requests", metricPath);
      requestTimes = solrMetricsContext.timer("requestTimes", metricPath);
      totalTime = solrMetricsContext.counter("totalTime", metricPath);
    }
  }

  public static SolrParams getSolrParamsFromNamedList(NamedList<?> args, String key) {
    Object o = args.get(key);
    if (o != null && o instanceof NamedList) {
      return ((NamedList<?>) o).toSolrParams();
    }
    return null;
  }

  public NamedList<?> getInitArgs() {
    return initArgs;
  }

  public abstract void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception;

  @Override
  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    HandlerMetrics metrics = getMetricsForThisRequest(req);
    metrics.requests.inc();

    Timer.Context timer = metrics.requestTimes.time();
    try {
      TestInjection.injectLeaderTragedy(req.getCore());
      if (pluginInfo != null && pluginInfo.attributes.containsKey(USEPARAM))
        req.getContext().put(USEPARAM, pluginInfo.attributes.get(USEPARAM));
      SolrPluginUtils.setDefaults(this, req, defaults, appends, invariants);
      req.getContext().remove(USEPARAM);
      rsp.setHttpCaching(httpCaching);
      handleRequestBody(req, rsp);
      // count timeouts
      NamedList<?> header = rsp.getResponseHeader();
      if (header != null) {
        if (Boolean.TRUE.equals(
            header.getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY))) {
          metrics.numTimeouts.mark();
          rsp.setHttpCaching(false);
        }
      }
    } catch (Exception e) {
      e = normalizeReceivedException(req, e);
      processErrorMetricsOnException(e, metrics);
      rsp.setException(e);
    } finally {
      long elapsed = timer.stop();
      metrics.totalTime.inc(elapsed);
    }
  }

  public static void processErrorMetricsOnException(Exception e, HandlerMetrics metrics) {
    boolean isClientError = false;
    if (e instanceof SolrException) {
      final SolrException se = (SolrException) e;
      if (se.code() == SolrException.ErrorCode.CONFLICT.code) {
        return;
      } else if (se.code() >= 400 && se.code() < 500) {
        isClientError = true;
      }
    }

    metrics.numErrors.mark();
    if (isClientError) {
      log.error("Client exception", e);
      metrics.numClientErrors.mark();
    } else {
      log.error("Server exception", e);
      metrics.numServerErrors.mark();
    }
  }

  public static Exception normalizeReceivedException(SolrQueryRequest req, Exception e) {
    if (req.getCore() != null) {
      assert req.getCoreContainer() != null;
      if (req.getCoreContainer().checkTragicException(req.getCore())) {
        return SolrException.wrapLuceneTragicExceptionIfNecessary(e);
      }
    }

    if (e instanceof SyntaxError) {
      return new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    return e;
  }

  /** The metrics to be used for this request. */
  public HandlerMetrics getMetricsForThisRequest(SolrQueryRequest req) {
    return this.metrics;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public abstract String getDescription();

  @Override
  public Category getCategory() {
    return Category.QUERY;
  }

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    return null;
  }

  /**
   * Get the request handler registered to a given name.
   *
   * <p>This function is thread safe.
   */
  public static SolrRequestHandler getRequestHandler(
      String handlerName, PluginBag<SolrRequestHandler> reqHandlers) {
    if (handlerName == null) return null;
    SolrRequestHandler handler = reqHandlers.get(handlerName);
    int idx = 0;
    if (handler == null) {
      for (; ; ) {
        idx = handlerName.indexOf('/', idx + 1);
        if (idx > 0) {
          String firstPart = handlerName.substring(0, idx);
          handler = reqHandlers.get(firstPart);
          if (handler == null) continue;
          if (handler instanceof NestedRequestHandler) {
            return ((NestedRequestHandler) handler).getSubHandler(handlerName.substring(idx));
          }
        } else {
          break;
        }
      }
    }
    return handler;
  }

  public void setPluginInfo(PluginInfo pluginInfo) {
    if (this.pluginInfo == null) this.pluginInfo = pluginInfo;
  }

  public PluginInfo getPluginInfo() {
    return pluginInfo;
  }

  @Override
  public Collection<Api> getApis() {
    return List.of(new ApiBag.ReqHandlerToApi(this, ApiBag.constructSpec(pluginInfo)));
  }

  /**
   * Checks whether the given request is an internal request to a shard. We rely on the fact that an
   * internal search request to a shard contains the param "isShard", and an internal update request
   * to a shard contains the param "distrib.from".
   *
   * @return true if request is internal
   */
  public static boolean isInternalShardRequest(SolrQueryRequest req) {
    return req.getParams().get(DistributedUpdateProcessor.DISTRIB_FROM) != null
        || "true".equals(req.getParams().get(ShardParams.IS_SHARD));
  }
}

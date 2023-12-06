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

import static org.apache.solr.util.stats.InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.routing.AffinityReplicaListTransformerFactory;
import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.client.solrj.routing.ReplicaListTransformerFactory;
import org.apache.solr.client.solrj.routing.RequestReplicaListTransformerGenerator;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.AllowListUrlChecker;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.stats.InstrumentedHttpListenerFactory;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpShardHandlerFactory extends ShardHandlerFactory
    implements org.apache.solr.util.plugin.PluginInfoInitialized, SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String DEFAULT_SCHEME = "http";

  // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
  //
  // This executor is initialized in the init method
  private ExecutorService commExecutor;

  protected volatile Http2SolrClient defaultClient;
  protected InstrumentedHttpListenerFactory httpListenerFactory;
  protected LBHttp2SolrClient loadbalancer;

  int corePoolSize = 0;
  int maximumPoolSize = Integer.MAX_VALUE;
  int keepAliveTime = 5;
  int queueSize = -1;
  int permittedLoadBalancerRequestsMinimumAbsolute = 0;
  float permittedLoadBalancerRequestsMaximumFraction = 1.0f;
  boolean accessPolicy = false;
  private SolrMetricsContext solrMetricsContext;

  private String scheme = null;

  private InstrumentedHttpListenerFactory.NameStrategy metricNameStrategy;

  protected final Random r = new Random();

  private RequestReplicaListTransformerGenerator requestReplicaListTransformerGenerator =
      new RequestReplicaListTransformerGenerator();

  // URL scheme to be used in distributed search.
  static final String INIT_URL_SCHEME = "urlScheme";

  // The core size of the threadpool servicing requests
  static final String INIT_CORE_POOL_SIZE = "corePoolSize";

  // The maximum size of the threadpool servicing requests
  static final String INIT_MAX_POOL_SIZE = "maximumPoolSize";

  // The amount of time idle threads persist for in the queue, before being killed
  static final String MAX_THREAD_IDLE_TIME = "maxThreadIdleTime";

  // If the threadpool uses a backing queue, what is its maximum size (-1) to use direct handoff
  static final String INIT_SIZE_OF_QUEUE = "sizeOfQueue";

  // The minimum number of replicas that may be used
  static final String LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE = "loadBalancerRequestsMinimumAbsolute";

  // The maximum proportion of replicas to be used
  static final String LOAD_BALANCER_REQUESTS_MAX_FRACTION = "loadBalancerRequestsMaximumFraction";

  // Configure if the threadpool favours fairness over throughput
  static final String INIT_FAIRNESS_POLICY = "fairnessPolicy";

  /** Get {@link ShardHandler} that uses the default http client. */
  @Override
  public ShardHandler getShardHandler() {
    return new HttpShardHandler(this);
  }

  private static NamedList<?> getNamedList(Object val) {
    if (val instanceof NamedList) {
      return (NamedList<?>) val;
    } else {
      throw new IllegalArgumentException(
          "Invalid config for replicaRouting; expected NamedList, but got " + val);
    }
  }

  private static String checkDefaultReplicaListTransformer(
      NamedList<?> c, String setTo, String extantDefaultRouting) {
    if (!Boolean.TRUE.equals(c.getBooleanArg("default"))) {
      return null;
    } else {
      if (extantDefaultRouting == null) {
        return setTo;
      } else {
        throw new IllegalArgumentException("more than one routing scheme marked as default");
      }
    }
  }

  private void initReplicaListTransformers(NamedList<?> routingConfig) {
    String defaultRouting = null;
    ReplicaListTransformerFactory stableRltFactory = null;
    ReplicaListTransformerFactory defaultRltFactory;
    if (routingConfig != null && routingConfig.size() > 0) {
      Iterator<? extends Entry<String, ?>> iter = routingConfig.iterator();
      do {
        Entry<String, ?> e = iter.next();
        String key = e.getKey();
        switch (key) {
          case ShardParams.REPLICA_RANDOM:
            // Only positive assertion of default status (i.e., default=true) is supported.
            // "random" is currently the implicit default, so explicitly configuring
            // "random" as default would not currently be useful, but if the implicit default
            // changes in the future, checkDefault could be relevant here.
            defaultRouting =
                checkDefaultReplicaListTransformer(getNamedList(e.getValue()), key, defaultRouting);
            break;
          case ShardParams.REPLICA_STABLE:
            NamedList<?> c = getNamedList(e.getValue());
            defaultRouting = checkDefaultReplicaListTransformer(c, key, defaultRouting);
            stableRltFactory = new AffinityReplicaListTransformerFactory(c);
            break;
          default:
            throw new IllegalArgumentException("invalid replica routing spec name: " + key);
        }
      } while (iter.hasNext());
    }
    if (stableRltFactory == null) {
      stableRltFactory = new AffinityReplicaListTransformerFactory();
    }
    if (ShardParams.REPLICA_STABLE.equals(defaultRouting)) {
      defaultRltFactory = stableRltFactory;
    } else {
      defaultRltFactory = RequestReplicaListTransformerGenerator.RANDOM_RLTF;
    }
    this.requestReplicaListTransformerGenerator =
        new RequestReplicaListTransformerGenerator(defaultRltFactory, stableRltFactory);
  }

  @Override
  public void init(PluginInfo info) {
    StringBuilder sb = new StringBuilder();
    NamedList<?> args = info.initArgs;
    this.scheme = getParameter(args, INIT_URL_SCHEME, null, sb);
    if (this.scheme != null && this.scheme.endsWith("://")) {
      this.scheme = this.scheme.replace("://", "");
    }

    String strategy =
        getParameter(
            args, "metricNameStrategy", UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY, sb);
    this.metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(strategy);
    if (this.metricNameStrategy == null) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "Unknown metricNameStrategy: "
              + strategy
              + " found. Must be one of: "
              + KNOWN_METRIC_NAME_STRATEGIES.keySet());
    }

    this.corePoolSize = getParameter(args, INIT_CORE_POOL_SIZE, corePoolSize, sb);
    this.maximumPoolSize = getParameter(args, INIT_MAX_POOL_SIZE, maximumPoolSize, sb);
    this.keepAliveTime = getParameter(args, MAX_THREAD_IDLE_TIME, keepAliveTime, sb);
    this.queueSize = getParameter(args, INIT_SIZE_OF_QUEUE, queueSize, sb);
    this.permittedLoadBalancerRequestsMinimumAbsolute =
        getParameter(
            args,
            LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE,
            permittedLoadBalancerRequestsMinimumAbsolute,
            sb);
    this.permittedLoadBalancerRequestsMaximumFraction =
        getParameter(
            args,
            LOAD_BALANCER_REQUESTS_MAX_FRACTION,
            permittedLoadBalancerRequestsMaximumFraction,
            sb);
    this.accessPolicy = getParameter(args, INIT_FAIRNESS_POLICY, accessPolicy, sb);

    if (args != null && args.get("shardsWhitelist") != null) {
      log.warn(
          "Property 'shardsWhitelist' is deprecated, please use '{}' instead.",
          AllowListUrlChecker.URL_ALLOW_LIST);
    }

    // magic sysprop to make tests reproducible: set by SolrTestCaseJ4.
    String v = System.getProperty("tests.shardhandler.randomSeed");
    if (v != null) {
      r.setSeed(Long.parseLong(v));
    }

    BlockingQueue<Runnable> blockingQueue =
        (this.queueSize == -1)
            ? new SynchronousQueue<Runnable>(this.accessPolicy)
            : new ArrayBlockingQueue<Runnable>(this.queueSize, this.accessPolicy);

    this.commExecutor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            this.corePoolSize,
            this.maximumPoolSize,
            this.keepAliveTime,
            TimeUnit.SECONDS,
            blockingQueue,
            new SolrNamedThreadFactory("httpShardExecutor"),
            // the Runnable added to this executor handles all exceptions so we disable stack trace
            // collection as an optimization. see SOLR-11880 for more details
            false);

    this.httpListenerFactory = new InstrumentedHttpListenerFactory(this.metricNameStrategy);
    int connectionTimeout =
        getParameter(
            args,
            HttpClientUtil.PROP_CONNECTION_TIMEOUT,
            HttpClientUtil.DEFAULT_CONNECT_TIMEOUT,
            sb);
    int maxConnectionsPerHost =
        getParameter(
            args,
            HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST,
            HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST,
            sb);
    int soTimeout =
        getParameter(args, HttpClientUtil.PROP_SO_TIMEOUT, HttpClientUtil.DEFAULT_SO_TIMEOUT, sb);

    this.defaultClient =
        new Http2SolrClient.Builder()
            .withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
            .withIdleTimeout(soTimeout, TimeUnit.MILLISECONDS)
            .withExecutor(commExecutor)
            .withMaxConnectionsPerHost(maxConnectionsPerHost)
            .build();
    this.defaultClient.addListenerFactory(this.httpListenerFactory);
    this.loadbalancer = new LBHttp2SolrClient.Builder(defaultClient).build();

    initReplicaListTransformers(getParameter(args, "replicaRouting", null, sb));

    log.debug("created with {}", sb);
  }

  @Override
  public void setSecurityBuilder(HttpClientBuilderPlugin clientBuilderPlugin) {
    if (clientBuilderPlugin != null) {
      clientBuilderPlugin.setup(defaultClient);
    }
  }

  protected <T> T getParameter(
      NamedList<?> initArgs, String configKey, T defaultValue, StringBuilder sb) {
    T toReturn = defaultValue;
    if (initArgs != null) {
      @SuppressWarnings({"unchecked"})
      T temp = (T) initArgs.get(configKey);
      toReturn = (temp != null) ? temp : defaultValue;
    }
    if (sb != null && toReturn != null)
      sb.append(configKey).append(" : ").append(toReturn).append(",");
    return toReturn;
  }

  @Override
  public void close() {
    try {
      if (loadbalancer != null) {
        loadbalancer.close();
      }
    } finally {
      try {
        if (defaultClient != null) {
          IOUtils.closeQuietly(defaultClient);
        }
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(commExecutor);
      }
    }
    try {
      SolrMetricProducer.super.close();
    } catch (Exception e) {
      log.warn("Exception closing.", e);
    }
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  protected LBSolrClient.Req newLBHttpSolrClientReq(final QueryRequest req, List<String> urls) {
    int numServersToTry =
        (int) Math.floor(urls.size() * this.permittedLoadBalancerRequestsMaximumFraction);
    if (numServersToTry < this.permittedLoadBalancerRequestsMinimumAbsolute) {
      numServersToTry = this.permittedLoadBalancerRequestsMinimumAbsolute;
    }
    return new LBSolrClient.Req(req, urls, numServersToTry);
  }

  /**
   * Creates a list of urls for the given shard.
   *
   * @param shard the urls for the shard, separated by '|'
   * @return A list of valid urls (including protocol) that are replicas for the shard
   */
  public List<String> buildURLList(String shard) {
    List<String> urls = StrUtils.splitSmart(shard, "|", true);

    // convert shard to URL
    for (int i = 0; i < urls.size(); i++) {
      urls.set(i, buildUrl(urls.get(i)));
    }

    return urls;
  }

  protected ReplicaListTransformer getReplicaListTransformer(final SolrQueryRequest req) {
    final SolrParams params = req.getParams();
    final SolrCore core = req.getCore(); // explicit check for null core (temporary?, for tests)
    @SuppressWarnings("resource")
    ZkController zkController = req.getCoreContainer().getZkController();
    if (zkController != null) {
      return requestReplicaListTransformerGenerator.getReplicaListTransformer(
          params,
          zkController
              .getZkStateReader()
              .getClusterProperties()
              .getOrDefault(ZkStateReader.DEFAULT_SHARD_PREFERENCES, "")
              .toString(),
          zkController.getNodeName(),
          zkController.getBaseUrl(),
          zkController.getSysPropsCacher());
    } else {
      return requestReplicaListTransformerGenerator.getReplicaListTransformer(params);
    }
  }

  public SolrClient getClient() {
    return defaultClient;
  }

  /**
   * Rebuilds the URL replacing the URL scheme of the passed URL with the configured scheme
   * replacement.If no scheme was configured, the passed URL's scheme is left alone.
   */
  private String buildUrl(String url) {
    if (!URLUtil.hasScheme(url)) {
      return (StrUtils.isNullOrEmpty(scheme) ? DEFAULT_SCHEME : scheme) + "://" + url;
    } else if (StrUtils.isNotNullOrEmpty(scheme)) {
      return scheme + "://" + URLUtil.removeScheme(url);
    }

    return url;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope = SolrMetricManager.mkName(scope, SolrInfoBean.Category.QUERY.name());
    httpListenerFactory.initializeMetrics(solrMetricsContext, expandedScope);
    commExecutor =
        MetricUtils.instrumentedExecutorService(
            commExecutor,
            null,
            solrMetricsContext.getMetricRegistry(),
            SolrMetricManager.mkName("httpShardExecutor", expandedScope, "threadPool"));
  }
}

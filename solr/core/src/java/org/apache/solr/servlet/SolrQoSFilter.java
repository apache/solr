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

package org.apache.solr.servlet;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.UnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quality-of-Service filter that asynchronously suspends requests when circuit breakers (global or
 * per-core) are tripped, instead of failing fast with a {@code 429}. Suspended requests are queued
 * by priority (and request type) and resumed by a periodic scheduler that re-checks the {@link
 * CircuitBreakerRegistry}; once no breaker is tripped for a request's type, the request is
 * dispatched to the rest of the chain.
 *
 * <p>Designed after Jetty's {@code QoSFilter}/{@code QoSHandler}: instead of dropping load during
 * transient spikes (e.g. GC pauses), the filter buffers requests, prioritizes intra-cluster shard /
 * admin / health-check traffic over user queries over background updates, and shed-loads only when
 * the suspension queue saturates or a request's suspension timeout elapses.
 *
 * <p>This filter always synchronously checks circuit breakers on the request path. When QoS
 * queueing is enabled (the default; toggle via {@value #SYSPROP_QOS_ENABLED}), a tripped breaker
 * causes the request to be suspended (via {@link HttpServletRequest#startAsync()}) and queued by
 * priority + type until the breaker clears. When QoS queueing is disabled, a tripped breaker causes
 * a synchronous fail-fast {@code 429} — preserving the legacy {@code SearchHandler} / {@code
 * ContentStreamHandlerBase} behavior that previously enforced the same check inside the handlers.
 *
 * <p>Internal intra-cluster shard requests (those carrying {@code Solr-Request-Context: SERVER})
 * always bypass both the breaker check and suspension to avoid distributed deadlock — sub-shard
 * requests cannot be queued waiting on a parent that is itself waiting.
 *
 * <p>All filters and the SolrServlet on the request path must declare {@code
 * <async-supported>true</async-supported>}; otherwise {@link HttpServletRequest#startAsync()} will
 * throw at runtime when a circuit breaker first trips.
 *
 * @lucene.experimental
 */
public class SolrQoSFilter extends CoreContainerAwareHttpFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SYSPROP_QOS_ENABLED =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.enabled";
  public static final String SYSPROP_MAX_SUSPENDED =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.maxSuspendedRequests";
  public static final String SYSPROP_SUSPEND_TIMEOUT_MS =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.suspendTimeoutMs";
  public static final String SYSPROP_CHECK_INTERVAL_MS =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.checkIntervalMs";
  public static final String SYSPROP_EVAL_INTERVAL_MS =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.evaluationIntervalMs";
  public static final String SYSPROP_DRAIN_BUDGET =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.drainBudget";
  public static final String SYSPROP_HIGH_SHARE =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.priority.high.share";
  public static final String SYSPROP_MEDIUM_SHARE =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.priority.medium.share";
  public static final String SYSPROP_HIGH_DRAIN_MULTIPLIER =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.drainBudget.highMultiplier";
  public static final String SYSPROP_LOW_DRAIN_MULTIPLIER =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "qos.drainBudget.lowMultiplier";

  /**
   * Optional client-supplied priority hint. Values are case-insensitive {@code HIGH}, {@code
   * MEDIUM}, or {@code LOW}. Unknown values are ignored and the filter falls back to its path /
   * type heuristic. Trusted callers (internal admin tooling, well-behaved SolrJ extensions) may use
   * this to override the default lane assignment without the filter having to inspect the request
   * body.
   */
  public static final String HEADER_REQUEST_PRIORITY = "Solr-Request-Priority";

  static final int DEFAULT_MAX_SUSPENDED_REQUESTS = 1024;
  static final long DEFAULT_SUSPEND_TIMEOUT_MS = 5000L;
  static final long DEFAULT_CHECK_INTERVAL_MS = 100L;
  static final long DEFAULT_EVAL_INTERVAL_MS = 200L;
  static final int DEFAULT_DRAIN_BUDGET = 200;
  static final double DEFAULT_HIGH_SHARE = 0.10;
  static final double DEFAULT_MEDIUM_SHARE = 0.60;
  static final double DEFAULT_HIGH_DRAIN_MULTIPLIER = 4.0;
  static final double DEFAULT_LOW_DRAIN_MULTIPLIER = 0.5;

  static final int PRIORITY_HIGH = 2; // admin/ping/health-check
  static final int PRIORITY_MEDIUM = 1; // user QUERY
  static final int PRIORITY_LOW = 0; // UPDATE
  static final int N_PRIORITIES = 3;

  static final int TYPE_QUERY = 0;
  static final int TYPE_UPDATE = 1;
  static final int N_TYPES = 2;

  private static final String ATTR_RESUMED = SolrQoSFilter.class.getName() + ".resumed";

  private static final String METRIC_PREFIX = "qos.";

  private boolean enabled;
  private int maxSuspendedRequests;
  private long suspendTimeoutMs;
  private long checkIntervalMs;
  private long evaluationIntervalNanos;
  private int drainBudget;
  private final int[] priorityCap = new int[N_PRIORITIES];
  private final int[] priorityDrainBudget = new int[N_PRIORITIES];

  // Cache of the last circuit-breaker scan per request type. Several admission-control checks
  // (load average, CPU) are expensive and sourced from metrics that don't update faster than
  // ~once per second anyway, so polling them per-request scales badly under load. We share
  // one evaluation across all callers within evaluationIntervalNanos.
  private volatile TrippedScan queryScan;
  private volatile TrippedScan updateScan;

  private final Queue<AsyncContext>[][] queues;
  private final AtomicInteger suspendedCount = new AtomicInteger();
  private final AtomicInteger[] suspendedByPriority = new AtomicInteger[N_PRIORITIES];
  private final AtomicLong totalSuspended = new AtomicLong();
  private final AtomicLong totalResumed = new AtomicLong();
  private final AtomicLong totalExpired = new AtomicLong();
  private final AtomicLong totalRejected = new AtomicLong();

  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> drainTask;

  private LongCounter metricSuspended;
  private LongCounter metricResumed;
  private LongCounter metricExpired;
  private LongCounter metricRejected;
  private ObservableLongGauge metricCurrentSuspendedGauge;

  // Test seam: when non-null, used in place of {@link #getCores()} so unit tests can run
  // without spinning up a full CoreContainerProvider/servlet context.
  private CoreContainer testCoreContainer;
  private boolean skipContainerLookup;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public SolrQoSFilter() {
    queues = new Queue[N_PRIORITIES][N_TYPES];
    for (int p = 0; p < N_PRIORITIES; p++) {
      suspendedByPriority[p] = new AtomicInteger();
      for (int t = 0; t < N_TYPES; t++) {
        queues[p][t] = new ConcurrentLinkedQueue<>();
      }
    }
  }

  private void registerMetrics() {
    SolrMetricManager mm;
    try {
      CoreContainer cc = resolveCoreContainer();
      mm = (cc == null) ? null : cc.getMetricManager();
    } catch (UnavailableException e) {
      log.warn("CoreContainer unavailable at SolrQoSFilter init; metrics not registered", e);
      return;
    }
    if (mm == null) {
      return;
    }
    String registry = SolrMetricManager.NODE_REGISTRY;
    metricSuspended =
        mm.longCounter(
            registry,
            METRIC_PREFIX + "suspended.total",
            "Requests suspended by SolrQoSFilter waiting for a tripped circuit breaker to clear",
            null);
    metricResumed =
        mm.longCounter(
            registry,
            METRIC_PREFIX + "resumed.total",
            "Suspended requests dispatched by SolrQoSFilter once their circuit breaker cleared",
            null);
    metricExpired =
        mm.longCounter(
            registry,
            METRIC_PREFIX + "expired.total",
            "Suspended requests rejected by SolrQoSFilter after the suspension timeout elapsed",
            null);
    metricRejected =
        mm.longCounter(
            registry,
            METRIC_PREFIX + "rejected.total",
            "Requests rejected fast by SolrQoSFilter because the suspension queue was full",
            null);
    metricCurrentSuspendedGauge =
        mm.observableLongGauge(
            registry,
            METRIC_PREFIX + "suspended.current",
            "Requests currently suspended by SolrQoSFilter",
            measurement -> measurement.record(suspendedCount.get()),
            null);
  }

  @Override
  public void destroy() {
    if (drainTask != null) {
      drainTask.cancel(false);
    }
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    if (metricCurrentSuspendedGauge != null) {
      metricCurrentSuspendedGauge.close();
    }
    super.destroy();
  }

  @Override
  protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    if (Boolean.TRUE.equals(req.getAttribute(ATTR_RESUMED))) {
      // Resumed dispatch from drainQueue — already passed the breaker check; let it through.
      chain.doFilter(req, res);
      return;
    }

    if (InternalRequestUtils.isInternalServerRequest(req)) {
      // Sub-shard requests must not be queued or fail-fast — the parent request is blocking on
      // them and refusing them would cause distributed deadlock.
      chain.doFilter(req, res);
      return;
    }

    SolrRequestType type = inferRequestType(req);
    CoreContainer cc;
    try {
      cc = resolveCoreContainer();
    } catch (UnavailableException ex) {
      // Container shutting down or not yet up — pass through.
      chain.doFilter(req, res);
      return;
    }
    List<CircuitBreaker> tripped = trippedCached(cc, type);
    if (tripped == null) {
      chain.doFilter(req, res);
      return;
    }

    if (!enabled) {
      // QoS queueing disabled: preserve the legacy SearchHandler / ContentStreamHandlerBase
      // behavior of failing fast with a 429 when a breaker is tripped.
      res.sendError(
          CircuitBreaker.getExceptionErrorCode().code,
          CircuitBreakerRegistry.toErrorMessage(tripped));
      return;
    }

    if (suspendedCount.get() >= maxSuspendedRequests) {
      rejectQueueFull(res, "Server overloaded; suspended-request queue full");
      return;
    }

    int priority = inferPriority(req, type);
    int typeIdx = typeIndex(type);
    if (suspendedByPriority[priority].get() >= priorityCap[priority]) {
      // Lane saturated — refuse fast, but only at this priority. A flood of LOW-priority work
      // cannot starve HIGH-priority probes because each lane has its own cap.
      rejectQueueFull(res, "Server overloaded; priority lane queue full");
      return;
    }

    AsyncContext asyncContext = req.startAsync();
    asyncContext.setTimeout(suspendTimeoutMs);
    asyncContext.addListener(new TimeoutListener(priority, typeIdx));
    suspendedCount.incrementAndGet();
    suspendedByPriority[priority].incrementAndGet();
    totalSuspended.incrementAndGet();
    if (metricSuspended != null) {
      metricSuspended.add(1);
    }
    queues[priority][typeIdx].add(asyncContext);
    if (log.isDebugEnabled()) {
      log.debug(
          "SolrQoSFilter suspended request priority={} type={} ({} now suspended)",
          priority,
          type,
          suspendedCount.get());
    }
  }

  private void rejectQueueFull(HttpServletResponse res, String message) throws IOException {
    totalRejected.incrementAndGet();
    if (metricRejected != null) {
      metricRejected.add(1);
    }
    res.sendError(CircuitBreaker.getExceptionErrorCode().code, message);
  }

  private void drainAll() {
    try {
      CoreContainer cc;
      try {
        cc = resolveCoreContainer();
      } catch (UnavailableException ex) {
        return;
      }
      for (int p = N_PRIORITIES - 1; p >= 0; p--) {
        for (int t = 0; t < N_TYPES; t++) {
          drainQueue(queues[p][t], p, typeFromIndex(t), cc, priorityDrainBudget[p]);
        }
      }
    } catch (Throwable t) {
      log.error("SolrQoSFilter drain failed", t);
    }
  }

  private void drainQueue(
      Queue<AsyncContext> q, int priority, SolrRequestType type, CoreContainer cc, int budgetCap) {
    if (q.isEmpty()) {
      return;
    }
    List<CircuitBreaker> tripped = trippedCached(cc, type);
    if (tripped != null) {
      return;
    }
    int budget = budgetCap;
    while (budget-- > 0) {
      AsyncContext popped = q.poll();
      if (popped == null) {
        return;
      }
      try {
        HttpServletRequest req = (HttpServletRequest) popped.getRequest();
        req.setAttribute(ATTR_RESUMED, Boolean.TRUE);
        popped.dispatch();
        suspendedCount.decrementAndGet();
        suspendedByPriority[priority].decrementAndGet();
        totalResumed.incrementAndGet();
        if (metricResumed != null) {
          metricResumed.add(1);
        }
      } catch (IllegalStateException ise) {
        // Async context already terminal (timed out or errored). The TimeoutListener already
        // decremented both counters; just discard the dead entry.
        if (log.isDebugEnabled()) {
          log.debug("SolrQoSFilter dispatch raced with completion", ise);
        }
      }
    }
  }

  /**
   * Determine request type from the {@code Solr-Request-Type} header, falling back to a crude path
   * heuristic. Anything not recognized as UPDATE is treated as QUERY for circuit-breaker
   * accounting.
   *
   * <p>The path fallback only matches paths containing {@code /update}; everything else (including
   * admin endpoints such as {@code /solr/admin/collections}, security APIs, and core admin actions)
   * is classified as QUERY. The current circuit-breaker registry only differentiates QUERY from
   * UPDATE breakers, so this coarseness is acceptable, but if a future breaker type is added (e.g.
   * ADMIN) this heuristic will need refinement.
   */
  static SolrRequestType inferRequestType(HttpServletRequest req) {
    String typeHeader = req.getHeader(CommonParams.SOLR_REQUEST_TYPE_PARAM);
    if (typeHeader != null) {
      try {
        SolrRequestType t = SolrRequestType.valueOf(typeHeader);
        if (t == SolrRequestType.UPDATE) {
          return SolrRequestType.UPDATE;
        }
        return SolrRequestType.QUERY;
      } catch (IllegalArgumentException ignored) {
        // fall through to path heuristic
      }
    }
    String path = req.getRequestURI();
    if (path != null && path.contains("/update")) {
      return SolrRequestType.UPDATE;
    }
    return SolrRequestType.QUERY;
  }

  /**
   * Higher priorities are dispatched first. The {@value #HEADER_REQUEST_PRIORITY} header wins when
   * present and recognized; otherwise admin/ping outranks user queries, and updates have the lowest
   * priority so background batch ingest yields to interactive load.
   *
   * <p>Internal cluster (server-context) traffic is detected separately by {@link
   * InternalRequestUtils#isInternalServerRequest(HttpServletRequest)} and bypasses suspension
   * entirely; it does not receive a priority here.
   */
  static int inferPriority(HttpServletRequest req, SolrRequestType type) {
    String hint = req.getHeader(HEADER_REQUEST_PRIORITY);
    if (hint != null) {
      switch (hint.toUpperCase(Locale.ROOT)) {
        case "HIGH":
          return PRIORITY_HIGH;
        case "MEDIUM":
          return PRIORITY_MEDIUM;
        case "LOW":
          return PRIORITY_LOW;
        default:
          // Unknown value — fall through to default heuristic instead of erroring on bad input.
      }
    }
    String path = req.getRequestURI();
    if (path != null && (path.contains("/admin/ping") || path.contains("/admin/info"))) {
      return PRIORITY_HIGH;
    }
    if (type == SolrRequestType.UPDATE) {
      return PRIORITY_LOW;
    }
    return PRIORITY_MEDIUM;
  }

  private static int typeIndex(SolrRequestType type) {
    return type == SolrRequestType.UPDATE ? TYPE_UPDATE : TYPE_QUERY;
  }

  private static SolrRequestType typeFromIndex(int idx) {
    return idx == TYPE_UPDATE ? SolrRequestType.UPDATE : SolrRequestType.QUERY;
  }

  public long getTotalSuspendedCount() {
    return totalSuspended.get();
  }

  public long getTotalResumedCount() {
    return totalResumed.get();
  }

  public long getTotalExpiredCount() {
    return totalExpired.get();
  }

  public long getTotalRejectedCount() {
    return totalRejected.get();
  }

  public int getSuspendedCount() {
    return suspendedCount.get();
  }

  @VisibleForTesting
  int getSuspendedCountForPriority(int priority) {
    return suspendedByPriority[priority].get();
  }

  @VisibleForTesting
  int getPriorityCap(int priority) {
    return priorityCap[priority];
  }

  @VisibleForTesting
  int getPriorityDrainBudget(int priority) {
    return priorityDrainBudget[priority];
  }

  @VisibleForTesting
  void drainNow() {
    drainAll();
  }

  @VisibleForTesting
  void initForTest(CoreContainer testContainer) {
    this.skipContainerLookup = true;
    this.testCoreContainer = testContainer;
    readConfig();
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
    super.init(config);
    readConfig();

    if (!enabled) {
      log.info(
          "SolrQoSFilter QoS queueing disabled; tripped circuit breakers will fail-fast "
              + "synchronously (set {}=true to enable async suspension instead)",
          SYSPROP_QOS_ENABLED);
      return;
    }
    registerMetrics();
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "solr-qos-drain");
              t.setDaemon(true);
              return t;
            });
    drainTask =
        scheduler.scheduleWithFixedDelay(
            this::drainAll, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
    log.info(
        "SolrQoSFilter enabled (maxSuspended={}, suspendTimeoutMs={}, checkIntervalMs={})",
        maxSuspendedRequests,
        suspendTimeoutMs,
        checkIntervalMs);
  }

  private void readConfig() {
    enabled = Boolean.parseBoolean(EnvUtils.getProperty(SYSPROP_QOS_ENABLED, "true"));
    maxSuspendedRequests =
        EnvUtils.getPropertyAsInteger(SYSPROP_MAX_SUSPENDED, DEFAULT_MAX_SUSPENDED_REQUESTS);
    suspendTimeoutMs =
        EnvUtils.getPropertyAsLong(SYSPROP_SUSPEND_TIMEOUT_MS, DEFAULT_SUSPEND_TIMEOUT_MS);
    checkIntervalMs =
        EnvUtils.getPropertyAsLong(SYSPROP_CHECK_INTERVAL_MS, DEFAULT_CHECK_INTERVAL_MS);
    evaluationIntervalNanos =
        TimeUnit.MILLISECONDS.toNanos(
            EnvUtils.getPropertyAsLong(SYSPROP_EVAL_INTERVAL_MS, DEFAULT_EVAL_INTERVAL_MS));
    String drainOverride = EnvUtils.getProperty(SYSPROP_DRAIN_BUDGET);
    if (drainOverride != null && !drainOverride.isEmpty()) {
      drainBudget = Integer.parseInt(drainOverride);
    } else {
      drainBudget = autoDrainBudget();
    }
    computePriorityCaps();
    computePriorityDrainBudgets();
  }

  /**
   * Choose a drain budget such that a fully-saturated queue can drain within the suspension timeout
   * (with 2× safety margin), so requests don't expire while waiting their turn. Floors at {@link
   * #DEFAULT_DRAIN_BUDGET} so small/default deployments behave exactly as before. Users with
   * workloads outside this envelope can override via {@value #SYSPROP_DRAIN_BUDGET}.
   *
   * <p>Without this scaling, very large {@code maxSuspendedRequests} configurations (e.g. 100k)
   * paired with the default 200 budget would let suspended requests sit past their timeout and fail
   * with a 429 — defeating the point of queueing them.
   */
  private int autoDrainBudget() {
    if (suspendTimeoutMs <= 0 || checkIntervalMs <= 0) {
      return DEFAULT_DRAIN_BUDGET;
    }
    long ticksPerTimeout = Math.max(1, suspendTimeoutMs / checkIntervalMs);
    long auto = 2L * maxSuspendedRequests / ticksPerTimeout;
    if (auto > Integer.MAX_VALUE) {
      auto = Integer.MAX_VALUE;
    }
    return Math.max(DEFAULT_DRAIN_BUDGET, (int) auto);
  }

  /**
   * Partition {@code maxSuspendedRequests} across the three priority lanes. HIGH and MEDIUM share
   * fractions are configurable; LOW takes the remainder. A flood of LOW-priority requests can fill
   * its lane but cannot evict HIGH/MEDIUM headroom — so admin/health-check probes still get queued
   * even during a sustained indexing storm.
   */
  private void computePriorityCaps() {
    double highShare = parseShare(SYSPROP_HIGH_SHARE, DEFAULT_HIGH_SHARE);
    double mediumShare = parseShare(SYSPROP_MEDIUM_SHARE, DEFAULT_MEDIUM_SHARE);
    int high = Math.max(1, (int) Math.round(maxSuspendedRequests * highShare));
    int medium = Math.max(1, (int) Math.round(maxSuspendedRequests * mediumShare));
    if (high + medium > maxSuspendedRequests) {
      // Shares exceed 100%; clamp so the sum can never exceed maxSuspendedRequests.
      medium = Math.max(0, maxSuspendedRequests - high);
      if (medium == 0) {
        high = maxSuspendedRequests;
      }
    }
    int low = Math.max(0, maxSuspendedRequests - high - medium);
    priorityCap[PRIORITY_HIGH] = high;
    priorityCap[PRIORITY_MEDIUM] = medium;
    priorityCap[PRIORITY_LOW] = low;
  }

  /**
   * Compute per-lane drain throughput. HIGH gets the bulk so probe traffic never queues long; LOW
   * gets the smallest share so updates back off gently after a breaker recovers. Defaults yield 800
   * / 200 / 100 dispatches per tick at the default {@code drainBudget=200}.
   */
  private void computePriorityDrainBudgets() {
    double highMultiplier =
        getPropertyAsDouble(SYSPROP_HIGH_DRAIN_MULTIPLIER, DEFAULT_HIGH_DRAIN_MULTIPLIER);
    double lowMultiplier =
        getPropertyAsDouble(SYSPROP_LOW_DRAIN_MULTIPLIER, DEFAULT_LOW_DRAIN_MULTIPLIER);
    priorityDrainBudget[PRIORITY_HIGH] =
        Math.max(1, (int) Math.round(drainBudget * highMultiplier));
    priorityDrainBudget[PRIORITY_MEDIUM] = Math.max(1, drainBudget);
    priorityDrainBudget[PRIORITY_LOW] = Math.max(1, (int) Math.round(drainBudget * lowMultiplier));
  }

  private static double parseShare(String prop, double dflt) {
    double v = getPropertyAsDouble(prop, dflt);
    if (v < 0.0) {
      return 0.0;
    }
    if (v > 1.0) {
      return 1.0;
    }
    return v;
  }

  private static double getPropertyAsDouble(String prop, double dflt) {
    String raw = EnvUtils.getProperty(prop);
    if (raw == null || raw.isEmpty()) {
      return dflt;
    }
    try {
      return Double.parseDouble(raw);
    } catch (NumberFormatException nfe) {
      log.warn("Invalid double for {}={}; using default {}", prop, raw, dflt);
      return dflt;
    }
  }

  private CoreContainer resolveCoreContainer() throws UnavailableException {
    if (skipContainerLookup) {
      return testCoreContainer;
    }
    return getCores();
  }

  /**
   * Cache the result of {@link CircuitBreakerRegistry#checkTrippedAcrossCores} for {@code
   * evaluationIntervalNanos} so that high-QPS request flows don't repeatedly hit expensive
   * underlying metrics. Each request type maintains its own cache slot; concurrent callers may
   * occasionally race and produce duplicate evaluations, which is bounded and acceptable.
   *
   * @return null when nothing is tripped; otherwise a non-empty list (matching the registry's
   *     contract — never an empty list).
   */
  private List<CircuitBreaker> trippedCached(CoreContainer cc, SolrRequestType type) {
    long now = System.nanoTime();
    TrippedScan cached = (type == SolrRequestType.UPDATE) ? updateScan : queryScan;
    if (cached != null && (now - cached.evaluatedNanos) < evaluationIntervalNanos) {
      return cached.tripped;
    }
    List<CircuitBreaker> fresh = CircuitBreakerRegistry.checkTrippedAcrossCores(cc, type);
    TrippedScan next = new TrippedScan(now, fresh);
    if (type == SolrRequestType.UPDATE) {
      updateScan = next;
    } else {
      queryScan = next;
    }
    return fresh;
  }

  private static final class TrippedScan {
    final long evaluatedNanos;
    final List<CircuitBreaker> tripped;

    TrippedScan(long evaluatedNanos, List<CircuitBreaker> tripped) {
      this.evaluatedNanos = evaluatedNanos;
      this.tripped = tripped;
    }
  }

  /**
   * AsyncListener for suspended requests. Both {@link #onTimeout} and {@link #onError} are terminal
   * states that account the request as expired and decrement {@link #suspendedCount}; the
   * underlying {@link AsyncContext} is left in its priority queue and will be polled and discarded
   * lazily by {@link #drainQueue} (which catches the resulting {@link IllegalStateException}). This
   * avoids the {@code O(N)} {@link ConcurrentLinkedQueue#remove(Object)} cost during overload.
   */
  private final class TimeoutListener implements AsyncListener {
    private final int priority;
    private final int typeIdx;

    TimeoutListener(int priority, int typeIdx) {
      this.priority = priority;
      this.typeIdx = typeIdx;
    }

    @Override
    public void onComplete(AsyncEvent event) {}

    @Override
    public void onStartAsync(AsyncEvent event) {}

    @Override
    public void onTimeout(AsyncEvent event) throws IOException {
      accountExpiration();
      ((HttpServletResponse) event.getSuppliedResponse())
          .sendError(
              CircuitBreaker.getExceptionErrorCode().code,
              "Server overloaded; suspended request timed out");
      event.getAsyncContext().complete();
    }

    @Override
    public void onError(AsyncEvent event) {
      // Client likely disconnected mid-suspension — release the slot. We cannot safely write to
      // the response here (it may already be unusable), so just account the expiration; the
      // container will complete the async cycle.
      accountExpiration();
    }

    private void accountExpiration() {
      suspendedCount.decrementAndGet();
      suspendedByPriority[priority].decrementAndGet();
      totalExpired.incrementAndGet();
      if (metricExpired != null) {
        metricExpired.add(1);
      }
      if (log.isDebugEnabled()) {
        log.debug(
            "SolrQoSFilter suspended request expired priority={} typeIdx={}", priority, typeIdx);
      }
    }
  }
}

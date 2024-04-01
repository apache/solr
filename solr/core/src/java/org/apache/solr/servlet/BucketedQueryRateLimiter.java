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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;
import org.apache.solr.client.solrj.request.beans.RateLimiterPayload;
import org.apache.solr.common.MapWriter;
import org.apache.solr.core.RateLimiterConfig;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implements a rate limiter with multiple named buckets
 */
public class BucketedQueryRateLimiter extends QueryRateLimiter implements SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Map<String, Class<? extends Condition>> conditionImpls =
      new LinkedHashMap<>();

  List<Bucket> buckets = new ArrayList<>();

  @Override
  public SlotMetadata handleRequest(RequestWrapper request) throws InterruptedException {
    for (Bucket bucket : buckets) {
      if (bucket.test(request)) {
        bucket.tries.increment();
        if (bucket.guaranteedSlotsPool.tryAcquire(
            bucket.bucketCfg.slotAcquisitionTimeoutInMS, TimeUnit.MILLISECONDS)) {
          bucket.success.increment();
          return bucket.guaranteedSlotMetadata;
        } else {
          bucket.fails.increment();
          // could not acquire  a slot
          return RequestRateLimiter.nullSlotMetadata;
        }
      }
    }
    // no bucket matches
    return null;
  }

  private SolrMetricsContext metrics;

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    metrics = parentContext.getChildContext(this);
    MetricsMap metricsMap =
        new MetricsMap(
            ew ->
                buckets.forEach(bucket -> ew.putNoEx(bucket.bucketCfg.name, bucket.getMetrics())));
    metrics.gauge(metricsMap, true, scope, null, SolrInfoBean.Category.CONTAINER.toString());
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return metrics;
  }

  static class Bucket {
    // for debugging purposes
    private final String cfg;
    private RateLimiterPayload.ReadBucketConfig bucketCfg;
    private final Semaphore guaranteedSlotsPool;
    private final SlotMetadata guaranteedSlotMetadata;
    private final List<Condition> conditions = new ArrayList<>();

    public LongAdder tries = new LongAdder();
    public LongAdder success = new LongAdder();
    public LongAdder fails = new LongAdder();
    public com.codahale.metrics.Timer tryWait = new com.codahale.metrics.Timer();

    private Bucket fallback;

    public boolean test(RequestWrapper req) {
      boolean isPass = true;
      for (Condition condition : conditions) {
        if (!condition.test(req)) return false;
      }
      return isPass;
    }

    public MapWriter getMetrics() {
      return ew -> {
        ew.put("queueLength", guaranteedSlotsPool.getQueueLength());
        ew.put("available", guaranteedSlotsPool.availablePermits());
        ew.put("tries", tries.longValue());
        ew.put("success", success.longValue());
        ew.put("fails", fails.longValue());
        ew.put("tryWaitAverage", tryWait.getMeanRate());
      };
    }

    public Bucket(RateLimiterPayload.ReadBucketConfig bucketCfg) {
      this.bucketCfg = bucketCfg;
      cfg = bucketCfg.jsonStr();
      this.guaranteedSlotsPool = new Semaphore(bucketCfg.allowedRequests);
      this.guaranteedSlotMetadata = new SlotMetadata(guaranteedSlotsPool);
    }

    @Override
    public String toString() {
      return cfg;
    }
  }

  public BucketedQueryRateLimiter(RateLimiterConfig rateLimiterConfig) {
    super(rateLimiterConfig);

    for (RateLimiterPayload.ReadBucketConfig bucketCfg : rateLimiterConfig.readBuckets) {
      Bucket b = new Bucket(bucketCfg);
      buckets.add(b);
      if (bucketCfg.header == null || bucketCfg.header.isEmpty()) {
        b.conditions.add(MatchAllCondition.INST);
        continue;
      }
      String k = bucketCfg.header.keySet().iterator().next();
      String v = bucketCfg.header.get(k);
      b.conditions.add(new HeaderCondition(k, v));
    }
  }

  interface Condition {

    boolean test(RequestWrapper req);
  }

  public static class HeaderCondition implements Condition {
    public static final String ID = "headerPattern";
    final String name, val;

    final Pattern valuePattern;

    public HeaderCondition(String name, String val) {
      this.name = name;
      this.val = val;
      valuePattern = Pattern.compile(val);
    }

    @Override
    public boolean test(RequestWrapper req) {
      String val = readVal(req);
      if (val == null) val = "";
      return valuePattern.matcher(val).find();
    }

    protected String readVal(RequestWrapper req) {
      return req.getHeader(name);
    }
  }

  public static class MatchAllCondition implements Condition {
    public static final MatchAllCondition INST = new MatchAllCondition();

    @Override
    public boolean test(RequestWrapper req) {
      return true;
    }
  }
}

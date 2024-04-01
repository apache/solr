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
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.RateLimiterConfig;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  static {
    conditionImpls.put(ReqHeaderCondition.ID, ReqHeaderCondition.class);
    conditionImpls.put(QueryParamCondition.ID, QueryParamCondition.class);
  }

  @SuppressWarnings("unchecked")
  public BucketedQueryRateLimiter(RateLimiterConfig rateLimiterConfig) {
    super(rateLimiterConfig);

    for (RateLimiterPayload.ReadBucketConfig bucketCfg : rateLimiterConfig.readBuckets) {
      Bucket b = new Bucket(bucketCfg);
      buckets.add(b);
      if (bucketCfg.conditions == null || bucketCfg.conditions.isEmpty()) {
        b.conditions.add(MatchAllCondition.INST);
        continue;
      }
      for (Object c : bucketCfg.conditions) {
        List<Object> conditionInfo = c instanceof List ? (List<Object>) c : List.of(c);
        for (Object o : conditionInfo) {
          Map<String, Object> info = (Map<String, Object>) o;
          Condition condition = null;

          for (Map.Entry<String, Class<? extends Condition>> e : conditionImpls.entrySet()) {
            if (info.containsKey(e.getKey())) {
              try {
                condition = e.getValue().getDeclaredConstructor().newInstance().init(info);
                break;
              } catch (Exception ex) {
                // unlikely
                throw new RuntimeException(ex);
              }
            }
          }
          if (condition == null) {
            throw new RuntimeException("Unknown condition : " + Utils.toJSONString(info));
          }
          b.conditions.add(condition);
        }
      }
    }
  }

  public interface Condition {
    boolean test(RequestWrapper req);

    Condition init(Map<String, Object> config);

    String identifier();
  }

  public abstract static class KeyValCondition implements Condition {
    protected String name;
    protected Pattern valuePattern;

    @SuppressWarnings("unchecked")
    @Override
    public Condition init(Map<String, Object> config) {
      Map<String, String> kv = (Map<String, String>) config.get(identifier());
      name = kv.keySet().iterator().next();
      valuePattern = Pattern.compile(kv.values().iterator().next());
      return this;
    }

    @Override
    public boolean test(RequestWrapper req) {
      String val = readVal(req);
      if (val == null) val = "";
      return valuePattern.matcher(val).find();
    }

    protected abstract String readVal(RequestWrapper req);
  }

  public static class ReqHeaderCondition extends KeyValCondition {
    public static final String ID = "headerPattern";

    @Override
    public String identifier() {
      return ID;
    }

    @Override
    protected String readVal(RequestWrapper req) {
      return req.getHeader(name);
    }
  }

  public static class QueryParamCondition extends KeyValCondition {
    public static final String ID = "queryParamPattern";

    @Override
    public String identifier() {
      return ID;
    }

    @Override
    protected String readVal(RequestWrapper req) {
      return req.getParameter(name);
    }
  }

  public static class MatchAllCondition implements Condition {
    public static final String ID = "";
    public static final MatchAllCondition INST = new MatchAllCondition();

    @Override
    public boolean test(RequestWrapper req) {
      return true;
    }

    @Override
    public Condition init(Map<String, Object> config) {
      return this;
    }

    @Override
    public String identifier() {
      return ID;
    }
  }
}

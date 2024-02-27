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

import com.codahale.metrics.Gauge;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpDestination;
import org.eclipse.jetty.client.HttpExchange;
import org.eclipse.jetty.client.api.Destination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shard handler factory that instruments the internal jetty http2 client to emit size metrics per
 * http destincation queue. This helps in identifying troublesome nodes in your Solr cloud setup.
 *
 * <p>It furthermore registers metrics for alive and zombie members of the shard ensemble reflecting
 * the current nodes view.
 *
 * <p>This code uses reflection to break up closed internal APIs.
 */
public class InstrumentedHtttpShardHandlerFactory extends HttpShardHandlerFactory
    implements SolrInfoBean {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ExecutorService scheduler;

  // store gauges here
  private final Map<String, HttpDestinationGauge> destinationGauges = new HashMap<>();

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);

    // a scheduler to check available HttpDestinations
    this.scheduler =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("httpshardhandler-destination-checker-"));

    registerDeadServerMetricGauges();
    registerHttpDestinationGauges(defaultClient);
  }

  void registerDeadServerMetricGauges() {
    try {
      // break up loadbalancer in super class
      final Field loadbalancer = HttpShardHandlerFactory.class.getDeclaredField("loadbalancer");
      loadbalancer.setAccessible(true);

      // break up zombie servers in LBHttp2SolrClient
      final Field zombieServers = LBSolrClient.class.getDeclaredField("zombieServers");
      zombieServers.setAccessible(true);

      // break up zombie servers in LBHttp2SolrClient
      final Field aliveServers = LBSolrClient.class.getDeclaredField("aliveServers");
      aliveServers.setAccessible(true);

      // get instance
      final LBHttp2SolrClient loadbalancerSolrClient = (LBHttp2SolrClient) loadbalancer.get(this);
      @SuppressWarnings("unchecked")
      final Map<String, Object> zombies =
          (Map<String, Object>) zombieServers.get(loadbalancerSolrClient);
      @SuppressWarnings("unchecked")
      final Map<String, Object> alives =
          (Map<String, Object>) aliveServers.get(loadbalancerSolrClient);

      // register the gauges
      getSolrMetricsContext()
          .gauge(
              () -> zombies.size(),
              true,
              "count",
              Category.QUERY.name(),
              "httpShardHandler",
              "zombieServers");
      getSolrMetricsContext()
          .gauge(
              () -> alives.size(),
              true,
              "count",
              Category.QUERY.name(),
              "httpShardHandler",
              "aliveServers");

    } catch (NoSuchFieldException
        | SecurityException
        | IllegalArgumentException
        | IllegalAccessException e) {
      if (log.isWarnEnabled()) {
        log.warn("Could not attach gauge to zombie server list", e);
      }
    }
  }

  void registerHttpDestinationGauges(Http2SolrClient solrClient) {
    try {
      // extract internal Jetty HttpClient
      Method getHttpClient = Http2SolrClient.class.getDeclaredMethod("getHttpClient");
      getHttpClient.setAccessible(true);
      final HttpClient httpClient = (HttpClient) getHttpClient.invoke(solrClient);

      // submit a thread to the single scheduler to poll
      // for new destinations every second
      scheduler.submit(
          () -> {
            while (!scheduler.isShutdown()) {
              // wait a second
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                if (log.isInfoEnabled()) {
                  log.info("Got interrupted while checking for new destinations to monitor ...");
                }
              }

              // check for new destinations
              try {
                checkAvailableHttpDestinations(httpClient);
              } catch (Exception e) {
                if (log.isWarnEnabled()) {
                  log.warn("Could not check available destinations", e);
                }
              }
            }
          });
    } catch (NoSuchMethodException
        | SecurityException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException e) {
      if (log.isWarnEnabled()) {
        log.warn("Could not attach gauge to HttpDestination queue", e);
      }
    }
  }

  void checkAvailableHttpDestinations(HttpClient httpClient) {
    for (Destination destination : httpClient.getDestinations()) {

      // build a finite destination name
      final String destinationName =
          String.format(
              "%s://%s:%s", destination.getScheme(), destination.getHost(), destination.getPort());

      // new destination appeared. Create Gauge
      if (!destinationGauges.containsKey(destinationName) && getSolrMetricsContext() != null) {
        try {
          final HttpDestinationGauge destinationGauge =
              new HttpDestinationGauge((HttpDestination) destination);
          getSolrMetricsContext()
              .gauge(
                  destinationGauge,
                  true,
                  "count",
                  Category.QUERY.name(),
                  "httpShardHandler",
                  "shardRequestQueue",
                  destinationName);
          destinationGauges.put(destinationName, destinationGauge);
        } catch (NoSuchFieldException | SecurityException e) {
          if (log.isWarnEnabled()) {
            log.warn("Could not inspect HttpDestination queue size", e);
          }
        }
      }
    }
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
    destinationGauges.clear();
  }

  public static class HttpDestinationGauge implements Gauge<Integer> {

    private final Field exchanges;
    private final HttpDestination destination;

    public HttpDestinationGauge(HttpDestination destination)
        throws NoSuchFieldException, SecurityException {
      this.destination = destination;

      // break up implementation
      this.exchanges = HttpDestination.class.getDeclaredField("exchanges");
      this.exchanges.setAccessible(true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Integer getValue() {
      try {
        return ((Queue<HttpExchange>) exchanges.get(destination)).size();
      } catch (IllegalArgumentException | IllegalAccessException e) {
        if (log.isInfoEnabled()) {
          log.info("Could not pull http request queue size", e);
        }

        return 0;
      }
    }
  }

  // SolrInfoMBean methods

  @Override
  public String getName() {
    return InstrumentedHtttpShardHandlerFactory.class.getName();
  }

  @Override
  public String getDescription() {
    return "A shard handler factory that captures Jetty http client metrics";
  }

  @Override
  public Category getCategory() {
    return Category.QUERY;
  }
}

package com.flipkart.solr.ltr.utils;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

public class MetricPublisher {
  private final MetricRegistry metricRegistry;
  private final String metricNamePrefix;

  public MetricPublisher(MetricRegistry metricRegistry, String metricNamePrefix) {
    this.metricRegistry = metricRegistry;
    this.metricNamePrefix = metricNamePrefix+".";
  }

  public void updateTimer(String name, long duration) {
    metricRegistry.timer(metricNamePrefix+name).update(duration, TimeUnit.MILLISECONDS);
  }

  public void updateHistogram(String name, int value) {
    metricRegistry.histogram(metricNamePrefix+name).update(value);
  }

  public void updateMeter(String name) {
    metricRegistry.meter(metricNamePrefix+name).mark();
  }

  public void register(String name, Metric metric) {
    String nameWithPrefix = metricNamePrefix+name;
    synchronized (metricRegistry) {
      metricRegistry.remove(nameWithPrefix);
      metricRegistry.register(nameWithPrefix, metric);
    }
  }
}

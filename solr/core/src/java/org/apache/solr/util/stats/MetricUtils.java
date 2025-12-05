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
package org.apache.solr.util.stats;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.util.NamedList;

/** Metrics specific utility functions. */
public class MetricUtils {

  public static final String VALUE = "value";

  private static final String MS = "_ms";

  private static final String MIN = "min";
  private static final String MIN_MS = MIN + MS;
  private static final String MAX = "max";
  private static final String MAX_MS = MAX + MS;
  private static final String MEAN = "mean";
  private static final String MEAN_MS = MEAN + MS;
  private static final String MEDIAN = "median";
  private static final String MEDIAN_MS = MEDIAN + MS;
  private static final String STDDEV = "stddev";
  private static final String STDDEV_MS = STDDEV + MS;
  private static final String SUM = "sum";
  private static final String P75 = "p75";
  private static final String P75_MS = P75 + MS;
  private static final String P95 = "p95";
  private static final String P95_MS = P95 + MS;
  private static final String P99 = "p99";
  private static final String P99_MS = P99 + MS;
  private static final String P999 = "p999";
  private static final String P999_MS = P999 + MS;

  /**
   * Adds metrics from a Timer to a NamedList, using well-known back-compat names.
   *
   * @param lst The NamedList to add the metrics data to
   * @param timer The Timer to extract the metrics from
   */
  public static void addMetrics(NamedList<Object> lst, Timer timer) {
    Snapshot snapshot = timer.getSnapshot();
    lst.add("avgRequestsPerSecond", timer.getMeanRate());
    lst.add("5minRateRequestsPerSecond", timer.getFiveMinuteRate());
    lst.add("15minRateRequestsPerSecond", timer.getFifteenMinuteRate());
    lst.add("avgTimePerRequest", nsToMs(snapshot.getMean()));
    lst.add("medianRequestTime", nsToMs(snapshot.getMedian()));
    lst.add("75thPcRequestTime", nsToMs(snapshot.get75thPercentile()));
    lst.add("95thPcRequestTime", nsToMs(snapshot.get95thPercentile()));
    lst.add("99thPcRequestTime", nsToMs(snapshot.get99thPercentile()));
    lst.add("999thPcRequestTime", nsToMs(snapshot.get999thPercentile()));
  }

  /**
   * Converts a double representing nanoseconds to a double representing milliseconds.
   *
   * @param ns the amount of time in nanoseconds
   * @return the amount of time in milliseconds
   */
  public static double nsToMs(double ns) {
    return ns / TimeUnit.MILLISECONDS.toNanos(1);
  }

  /**
   * Converts bytes to megabytes.
   *
   * @param bytes the number of bytes
   * @return the number of megabytes
   */
  public static double bytesToMegabytes(long bytes) {
    return bytes / (1024.0 * 1024.0);
  }

  /**
   * These are well-known implementations of {@link java.lang.management.OperatingSystemMXBean}.
   * Some of them provide additional useful properties beyond those declared by the interface.
   */
  public static String[] OS_MXBEAN_CLASSES =
      new String[] {
        OperatingSystemMXBean.class.getName(),
        "com.sun.management.OperatingSystemMXBean",
        "com.sun.management.UnixOperatingSystemMXBean",
        "com.ibm.lang.management.OperatingSystemMXBean"
      };
}

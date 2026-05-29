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
package org.apache.solr.security.agent;

import java.util.concurrent.atomic.LongAdder;

/**
 * Per-type violation counters incremented by the agent interceptors.
 *
 * <p>{@code AgentViolationMetrics} (in {@code solr:core}) reads these counters reflectively and
 * registers them as a single labeled OTel observable counter.
 */
public final class ViolationMetricsReporter {

  private static final LongAdder FILE_COUNTER = new LongAdder();
  private static final LongAdder NETWORK_COUNTER = new LongAdder();
  private static final LongAdder EXIT_COUNTER = new LongAdder();
  private static final LongAdder EXEC_COUNTER = new LongAdder();

  private ViolationMetricsReporter() {}

  public static void incrementFile() {
    FILE_COUNTER.increment();
  }

  public static void incrementNetwork() {
    NETWORK_COUNTER.increment();
  }

  public static void incrementExit() {
    EXIT_COUNTER.increment();
  }

  public static void incrementExec() {
    EXEC_COUNTER.increment();
  }

  public static long fileCount() {
    return FILE_COUNTER.sum();
  }

  public static long networkCount() {
    return NETWORK_COUNTER.sum();
  }

  public static long exitCount() {
    return EXIT_COUNTER.sum();
  }

  public static long execCount() {
    return EXEC_COUNTER.sum();
  }
}

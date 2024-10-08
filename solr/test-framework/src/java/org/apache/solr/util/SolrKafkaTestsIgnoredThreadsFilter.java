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
package org.apache.solr.util;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * This ignores those Kafka threads in Solr for which there is no way to clean up after a suite.
 * This is included in the test framework, so it can be used in tests for both the cross-dc module
 * and the cross-dc-manager.
 */
public class SolrKafkaTestsIgnoredThreadsFilter implements ThreadFilter {
  @Override
  public boolean reject(Thread t) {

    String threadName = t.getName();

    if (threadName.startsWith("metrics-meter-tick-thread")) {
      return true;
    }

    if (threadName.startsWith("pool-")) {
      return true;
    }

    if (threadName.startsWith("kafka-")) { // TODO
      return true;
    }

    if (threadName.startsWith("KafkaCrossDcConsumerWorker")) {
      return true;
    }

    return false;
  }
}

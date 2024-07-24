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
package org.apache.solr.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Snapshot;
import java.io.Closeable;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.junit.Test;

/** */
public class MaxHistogramTest extends SolrTestCaseJ4 {
  @Test
  @SuppressWarnings("try")
  public void test() throws Exception {
    MaxHistogram h =
        MaxHistogram.newInstance(
            1000,
            Integer.MAX_VALUE,
            Clock.defaultClock(),
            (clock) -> new SlidingTimeWindowArrayReservoir(5, TimeUnit.SECONDS, clock));
    ExecutorService exec = ExecutorUtil.newMDCAwareCachedThreadPool("maxHist");
    Random r = random();
    int nThreads = r.nextInt(50);
    try (Closeable c = () -> ExecutorUtil.shutdownAndAwaitTermination(exec)) {
      AtomicBoolean exit = new AtomicBoolean();
      @SuppressWarnings("rawtypes")
      Future<?>[] futures = new Future[nThreads];
      for (int i = 0; i < nThreads; i++) {
        Random tRandom = new Random(r.nextLong());
        futures[i] =
            exec.submit(
                () -> {
                  while (!exit.get()) {
                    h.update(1);
                    Thread.sleep(tRandom.nextInt(1000));
                    h.update(-1);
                    Thread.sleep(tRandom.nextInt(5000));
                  }
                  return null;
                });
      }
      for (int i = 0; i < 200; i++) {
        Thread.sleep(500);
        Snapshot s = h.getSnapshot();
        System.err.println(
            s.getMin()
                + ", "
                + s.getMedian()
                + ", "
                + s.getMean()
                + ", "
                + s.getMax()
                + ", "
                + s.getStdDev());
      }
      System.err.println("exiting...");
      exit.set(true);
      for (Future<?> f : futures) {
        f.get();
      }
    }
  }
}

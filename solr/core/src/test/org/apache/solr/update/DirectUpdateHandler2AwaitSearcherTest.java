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
package org.apache.solr.update;

import java.util.concurrent.FutureTask;
import org.apache.solr.SolrTestCase;

/** SOLR-7022: waiting for a searcher must not swallow an interrupt or log it as an error. */
public class DirectUpdateHandler2AwaitSearcherTest extends SolrTestCase {

  public void testInterruptStatusIsRestored() {
    // a task that never completes, so get() blocks (and sees the pending interrupt immediately)
    FutureTask<Object> neverDone = new FutureTask<>(() -> null);
    Thread.currentThread().interrupt();
    try {
      DirectUpdateHandler2.awaitSearcher(neverDone);
      assertTrue("interrupt status must be restored", Thread.currentThread().isInterrupted());
    } finally {
      // clear the flag so the test framework is not affected
      Thread.interrupted();
    }
  }

  public void testCompletedFutureReturns() {
    FutureTask<Object> done = new FutureTask<>(() -> null);
    done.run();
    DirectUpdateHandler2.awaitSearcher(done);
    assertFalse(Thread.currentThread().isInterrupted());
  }
}

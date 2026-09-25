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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.suggest.DictionaryFactory;

/**
 * Test-only dictionary that stands in for a suggester source whose reads are throttled by slow
 * storage (e.g. an Azure disk with exhausted burst credits): it sleeps a fixed amount of time
 * before emitting each of a fixed number of terms. The total delay is deterministic and bounded
 * (numTerms * sleepMs), unlike a real disk stall, so tests built on it stay fast and reproducible.
 */
public class SlowIOSimulatingDictionaryFactory extends DictionaryFactory {
  public static final String NUM_TERMS_PARAM = "slowDictNumTerms";
  public static final String SLEEP_MS_PARAM = "slowDictSleepMs";

  // Test hook: counts down once the dictionary created by the most recent call to create() has
  // been fully iterated, i.e. the suggester build that consumed it has read every term. Reset on
  // every create() call, so it reflects the most recently started build.
  private static volatile CountDownLatch dictionaryFullyReadLatch = new CountDownLatch(1);

  /**
   * Blocks until the most recently created dictionary has been fully iterated, or the timeout
   * elapses. Used by tests to detect that an asynchronous suggester build actually completed.
   */
  public static boolean awaitDictionaryFullyRead(long timeout, TimeUnit unit)
      throws InterruptedException {
    return dictionaryFullyReadLatch.await(timeout, unit);
  }

  @Override
  public Dictionary create(SolrCore core, SolrIndexSearcher searcher) {
    if (params == null) {
      throw new IllegalStateException("Value of params not set");
    }
    String name = (String) params.get(CommonParams.NAME);
    if (name == null) {
      throw new IllegalArgumentException(CommonParams.NAME + " is a mandatory parameter");
    }
    int numTerms = Integer.parseInt((String) params.get(NUM_TERMS_PARAM));
    long sleepMs = Long.parseLong((String) params.get(SLEEP_MS_PARAM));
    CountDownLatch latch = new CountDownLatch(1);
    dictionaryFullyReadLatch = latch;
    return new SlowDictionary(numTerms, sleepMs, latch);
  }

  private static class SlowDictionary implements Dictionary {
    private final int numTerms;
    private final long sleepMs;
    private final CountDownLatch fullyReadLatch;

    SlowDictionary(int numTerms, long sleepMs, CountDownLatch fullyReadLatch) {
      this.numTerms = numTerms;
      this.sleepMs = sleepMs;
      this.fullyReadLatch = fullyReadLatch;
    }

    @Override
    public InputIterator getEntryIterator() throws IOException {
      return new InputIterator.InputIteratorWrapper(
          new SlowByteRefIterator(numTerms, sleepMs, fullyReadLatch));
    }
  }

  private static class SlowByteRefIterator implements BytesRefIterator {
    private final int numTerms;
    private final long sleepMs;
    private final CountDownLatch fullyReadLatch;
    private int emitted = 0;

    SlowByteRefIterator(int numTerms, long sleepMs, CountDownLatch fullyReadLatch) {
      this.numTerms = numTerms;
      this.sleepMs = sleepMs;
      this.fullyReadLatch = fullyReadLatch;
    }

    @Override
    public BytesRef next() throws IOException {
      if (emitted >= numTerms) {
        fullyReadLatch.countDown();
        return null;
      }
      try {
        // simulate a slow, disk-I/O-bound read for this dictionary entry
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      ++emitted;
      return new BytesRef("slowterm" + emitted);
    }
  }
}

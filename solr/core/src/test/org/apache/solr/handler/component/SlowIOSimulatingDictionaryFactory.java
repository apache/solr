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
    return new SlowDictionary(numTerms, sleepMs);
  }

  private static class SlowDictionary implements Dictionary {
    private final int numTerms;
    private final long sleepMs;

    SlowDictionary(int numTerms, long sleepMs) {
      this.numTerms = numTerms;
      this.sleepMs = sleepMs;
    }

    @Override
    public InputIterator getEntryIterator() throws IOException {
      return new InputIterator.InputIteratorWrapper(new SlowByteRefIterator(numTerms, sleepMs));
    }
  }

  private static class SlowByteRefIterator implements BytesRefIterator {
    private final int numTerms;
    private final long sleepMs;
    private int emitted = 0;

    SlowByteRefIterator(int numTerms, long sleepMs) {
      this.numTerms = numTerms;
      this.sleepMs = sleepMs;
    }

    @Override
    public BytesRef next() throws IOException {
      if (emitted >= numTerms) {
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

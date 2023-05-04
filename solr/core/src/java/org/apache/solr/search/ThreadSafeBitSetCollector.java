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
package org.apache.solr.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.FixedBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 *
 */

public class ThreadSafeBitSetCollector extends SimpleCollector {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  final ThreadSafeBitSet bits;
  final int maxDoc;

  int base;



  public ThreadSafeBitSetCollector(ThreadSafeBitSet bits, int maxDoc) {
    this.bits = bits;
    this.maxDoc = maxDoc;
  }


  @Override
  public void collect(int doc) throws IOException {

    doc += base;
    log.error("collect doc {} {}", (doc + base), this);
      bits.set(doc);

  }

  /** The number of documents that have been collected */
  public int size() {
    return maxDoc;
  }

  public DocSet getDocSet() {
    log.error("Max Set Bit {}", bits.maxSetBit());

    FixedBitSet fixedBitSet = new FixedBitSet(maxDoc + 1);
    int cnt = 0;
    int i = -1;
    while (true) {
      i = bits.nextSetBit(i+1);
      if (i == -1) {
        break;
      }
      cnt++;
      fixedBitSet.set(i);
    }

    return new BitDocSet(fixedBitSet, cnt);

  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    this.base = context.docBase;
    log.error("next reader base=" + base);
  }

}

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

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

/** A simple delegating collector where one can set the delegate after creation */
public class DelegatingCollector extends SimpleCollector {

  /* for internal testing purposes only to determine the number of times a delegating collector chain was used */
  public static int setLastDelegateCount;

  protected Collector delegate;
  protected LeafCollector leafDelegate;
  protected Scorable scorer;
  protected LeafReaderContext context;
  protected int docBase;

  public Collector getDelegate() {
    return delegate;
  }

  public void setDelegate(Collector delegate) {
    this.delegate = delegate;
  }

  /** Sets the last delegate in a chain of DelegatingCollectors */
  public void setLastDelegate(Collector delegate) {
    DelegatingCollector ptr = this;
    for (;
        ptr.getDelegate() instanceof DelegatingCollector;
        ptr = (DelegatingCollector) ptr.getDelegate())
      ;
    ptr.setDelegate(delegate);
    setLastDelegateCount++;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    this.scorer = scorer;
    if (leafDelegate != null) {
      leafDelegate.setScorer(scorer);
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return delegate.scoreMode();
  }

  @Override
  public void collect(int doc) throws IOException {
    leafDelegate.collect(doc);
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    this.context = context;
    this.docBase = context.docBase;
    leafDelegate = delegate.getLeafCollector(context);
  }

  /**
   * From Solr 9.4 using Lucene 9.8 onwards <code>DelegatingCollector.finish</code> clashes with the
   * super class's <code>LeafCollector.finish</code> method. Please relocate any finishing logic
   * into the <code>DelegatingCollector.complete</code> replacement completion method.
   */
  @Override
  public final void finish() throws IOException {
    if (leafDelegate != null) {
      leafDelegate.finish();
    }
    super.finish();
  }

  /** since 9.4 */
  public void complete() throws IOException {
    if (delegate instanceof DelegatingCollector) {
      ((DelegatingCollector) delegate).complete();
    }
  }
}

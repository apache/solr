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

package org.apache.solr.client.solrj.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;

public class DebugAsyncListener implements AsyncListener<NamedList<Object>>, PauseableHttpSolrClient {

  private final CountDownLatch cdl;

  public volatile boolean onStartCalled;

  public volatile boolean latchCounted;

  public volatile NamedList<Object> onSuccessResult = null;

  public volatile Throwable onFailureResult = null;

  public DebugAsyncListener(CountDownLatch cdl) {
    this.cdl = cdl;
  }

  @Override
  public void onStart() {
    onStartCalled = true;
  }

  @Override
  public void onSuccess(NamedList<Object> entries) {
    pause();
    onSuccessResult = entries;
    if (latchCounted) {
      Assert.fail("either 'onSuccess' or 'onFailure' should be called exactly once.");
    }
    cdl.countDown();
    latchCounted = true;
    unPause();
  }

  @Override
  public void onFailure(Throwable throwable) {
    pause();
    onFailureResult = throwable;
    if (latchCounted) {
      Assert.fail("either 'onSuccess' or 'onFailure' should be called exactly once.");
    }
    cdl.countDown();
    latchCounted = true;
    unPause();
  }

  @Override
  public CompletableFuture<NamedList<Object>> requestAsync(SolrRequest<?> solrRequest, String collection) {
    throw new UnsupportedOperationException();
  }
}

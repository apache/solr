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

package org.apache.solr.client.solrj.jetty;

import org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClientTestBase;
import org.apache.solr.client.solrj.impl.HttpSolrClientBase;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentUpdateJettySolrClientTest extends ConcurrentUpdateSolrClientTestBase {

  @Override
  public ConcurrentUpdateBaseSolrClient outcomeCountingConcurrentClient(
      String serverUrl,
      int queueSize,
      int threadCount,
      HttpSolrClientBase solrClient,
      AtomicInteger successCounter,
      AtomicInteger failureCounter,
      StringBuilder errors) {
    return new OutcomeCountingConcurrentUpdateJettySolrClient.Builder(serverUrl, (HttpJettySolrClient) solrClient, successCounter, failureCounter, errors)
        .withQueueSize(queueSize)
        .withThreadCount(threadCount)
        .setPollQueueTime(0, TimeUnit.MILLISECONDS)
        .build();
  }

  @Override
  public HttpSolrClientBase solrClient(Integer overrideIdleTimeoutMs) {
    var builder = new HttpJettySolrClient.Builder();
    if(overrideIdleTimeoutMs != null) {
      builder.withIdleTimeout(overrideIdleTimeoutMs,  TimeUnit.MILLISECONDS);
    }
    return builder.build();
  }

  @Override
  public ConcurrentUpdateBaseSolrClient concurrentClient(
      HttpSolrClientBase solrClient,
      String baseUrl,
      String defaultCollection,
      int queueSize,
      int threadCount,
      boolean disablePollQueue) {
    var builder =
        new ConcurrentUpdateJettySolrClient.Builder(baseUrl, (HttpJettySolrClient) solrClient)
            .withQueueSize(queueSize)
            .withThreadCount(threadCount);
    if (defaultCollection != null) {
      builder.withDefaultCollection(defaultCollection);
    }
    if(disablePollQueue) {
      builder.setPollQueueTime(0, TimeUnit.MILLISECONDS);
    }
    return builder.build();
  }

  public static class OutcomeCountingConcurrentUpdateJettySolrClient extends ConcurrentUpdateJettySolrClient {
    private final AtomicInteger successCounter;
    private final AtomicInteger failureCounter;
    private final StringBuilder errors;

    public OutcomeCountingConcurrentUpdateJettySolrClient(
        OutcomeCountingConcurrentUpdateJettySolrClient.Builder builder) {
      super(builder);
      this.successCounter = builder.successCounter;
      this.failureCounter = builder.failureCounter;
      this.errors = builder.errors;
    }

    @Override
    public void handleError(Throwable ex) {
      failureCounter.incrementAndGet();
      errors.append(" " + ex);
    }

    @Override
    public void onSuccess(Object responseMetadata, InputStream respBody) {
      successCounter.incrementAndGet();
    }

    public static class Builder extends ConcurrentUpdateJettySolrClient.Builder {
      protected final AtomicInteger successCounter;
      protected final AtomicInteger failureCounter;
      protected final StringBuilder errors;

      public Builder(
          String baseSolrUrl,
          HttpJettySolrClient http2Client,
          AtomicInteger successCounter,
          AtomicInteger failureCounter,
          StringBuilder errors) {
        super(baseSolrUrl, http2Client);
        this.successCounter = successCounter;
        this.failureCounter = failureCounter;
        this.errors = errors;
      }

      @Override
      public OutcomeCountingConcurrentUpdateJettySolrClient build() {
        return new OutcomeCountingConcurrentUpdateJettySolrClient(this);
      }
    }
  }
}

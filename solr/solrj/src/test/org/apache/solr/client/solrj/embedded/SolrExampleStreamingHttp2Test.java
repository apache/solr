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

package org.apache.solr.client.solrj.embedded;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.jetty.ConcurrentUpdateJettySolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.XMLRequestWriter;
import org.apache.solr.client.solrj.response.XMLResponseParser;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;

/**
 * A subclass of SolrExampleTests that explicitly uses the HTTP2 client and the streaming update
 * client for communication.
 */
public class SolrExampleStreamingHttp2Test extends SolrExampleTests {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Override
  public SolrClient createNewSolrClient() {
    String url = getBaseUrl();
    // smaller queue size hits locks more often
    var solrClient =
        new HttpJettySolrClient.Builder()
            .withRequestWriter(new XMLRequestWriter())
            .withResponseParser(new XMLResponseParser())
            .build();
    var concurrentClient =
        new ErrorTrackingConcurrentUpdateSolrClient.Builder(url, solrClient)
            .withDefaultCollection(DEFAULT_TEST_CORENAME)
            .withQueueSize(2)
            .withThreadCount(5)
            .build();
    return concurrentClient;
  }

  public void testWaitOptions() throws Exception {
    // SOLR-3903
    final List<Throwable> failures = new ArrayList<>();
    final String serverUrl = getBaseUrl();
    try (var http2Client = new HttpJettySolrClient.Builder().build();
        var concurrentClient =
            new FailureRecordingConcurrentUpdateSolrClient.Builder(serverUrl, http2Client)
                .withDefaultCollection(DEFAULT_TEST_CORENAME)
                .withQueueSize(2)
                .withThreadCount(2)
                .build()) {
      int docId = 42;
      for (AbstractUpdateRequest.ACTION action : EnumSet.allOf(UpdateRequest.ACTION.class)) {
        for (boolean waitSearch : Arrays.asList(true, false)) {
          for (boolean waitFlush : Arrays.asList(true, false)) {
            UpdateRequest updateRequest = new UpdateRequest();
            SolrInputDocument document = new SolrInputDocument();
            document.addField("id", docId++);
            updateRequest.add(document);
            updateRequest.setAction(action, waitSearch, waitFlush);
            concurrentClient.request(updateRequest);
          }
        }
      }
      concurrentClient.commit();
      concurrentClient.blockUntilFinished();
    }

    if (0 != failures.size()) {
      assertNull(failures.size() + " Unexpected Exception, starting with...", failures.get(0));
    }
  }

  static class FailureRecordingConcurrentUpdateSolrClient extends ConcurrentUpdateJettySolrClient {
    private final List<Throwable> failures = new ArrayList<>();

    public FailureRecordingConcurrentUpdateSolrClient(
        ConcurrentUpdateJettySolrClient.Builder builder) {
      super(builder);
    }

    @Override
    public void handleError(Throwable ex) {
      failures.add(ex);
    }

    static class Builder extends ConcurrentUpdateJettySolrClient.Builder {
      public Builder(String baseSolrUrl, HttpJettySolrClient http2Client) {
        super(baseSolrUrl, http2Client);
      }

      @Override
      public FailureRecordingConcurrentUpdateSolrClient build() {
        return new FailureRecordingConcurrentUpdateSolrClient(this);
      }
    }
  }

  public static class ErrorTrackingConcurrentUpdateSolrClient
      extends ConcurrentUpdateJettySolrClient {
    public Throwable lastError = null;

    public ErrorTrackingConcurrentUpdateSolrClient(
        ConcurrentUpdateJettySolrClient.Builder builder) {
      super(builder);
    }

    @Override
    public void handleError(Throwable ex) {
      lastError = ex;
    }

    public static class Builder extends ConcurrentUpdateJettySolrClient.Builder {

      public Builder(String baseSolrUrl, HttpJettySolrClient http2Client) {
        super(baseSolrUrl, http2Client, true);
      }

      @Override
      public ErrorTrackingConcurrentUpdateSolrClient build() {
        return new ErrorTrackingConcurrentUpdateSolrClient(this);
      }
    }
  }
}

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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FullThrottleStoppableIndexingThread extends StoppableIndexingThread {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /**
   * 
   */
  private volatile boolean stop = false;
  int clientIndex = 0;
  private ConcurrentUpdateHttp2SolrClient cusc;
  private List<SolrClient> clients;
  private AtomicInteger fails = new AtomicInteger();

  public FullThrottleStoppableIndexingThread(SolrClient controlClient, CloudHttp2SolrClient cloudClient, List<SolrClient> clients,
                                             String id, boolean doDeletes, int clientSoTimeout) {
    super(controlClient, cloudClient, id, doDeletes);
    this.clients = clients;

    // HTTP/2: timeouts are configured on the underlying Http2SolrClient (the HTTP/1 client was removed).
    Http2SolrClient http2Client = new Http2SolrClient.Builder()
        .connectionTimeout(10000)
        .idleTimeout(clientSoTimeout)
        .build();
    cusc = new ErrorLoggingConcurrentUpdateSolrClient.Builder(
        ((Http2SolrClient) clients.get(0)).getBaseURL(), http2Client)
        .withQueueSize(8)
        .withThreadCount(2)
        .build();
  }
  
  @Override
  public void run() {
    int i = 0;
    int numDeletes = 0;
    int numAdds = 0;

    while (true && !stop) {
      String id = this.id + "-" + i;
      ++i;
      
      if (doDeletes && SolrTestCase.random().nextBoolean() && deletes.size() > 0) {
        String delete = deletes.remove(0);
        try {
          numDeletes++;
          cusc.deleteById(delete);
        } catch (Exception e) {
          changeUrlOnError(e);
          fails.incrementAndGet();
        }
      }
      
      try {
        numAdds++;
        if (numAdds > (LuceneTestCase.TEST_NIGHTLY ? 4002 : 197))
          continue;
        SolrInputDocument doc = AbstractFullDistribZkTestBase.getDoc(
            "id",
            id,
            i1,
            50,
            t1,
            "Saxon heptarchies that used to rip around so in old times and raise Cain.  My, you ought to seen old Henry the Eight when he was in bloom.  He WAS a blossom.  He used to marry a new wife every day, and chop off her head next morning.  And he would do it just as indifferent as if ");
        cusc.add(doc);
      } catch (Exception e) {
        changeUrlOnError(e);
        fails.incrementAndGet();
      }
      
      if (doDeletes && SolrTestCase.random().nextBoolean()) {
        deletes.add(id);
      }
      
    }

    log.info("FT added docs:{} with {} fails deletes:{}", numAdds, fails, numDeletes);
  }

  private void changeUrlOnError(Exception e) {
    if (e instanceof ConnectException) {
      clientIndex++;
      if (clientIndex > clients.size() - 1) {
        clientIndex = 0;
      }
      cusc.shutdownNow();
      Http2SolrClient http2Client = new Http2SolrClient.Builder().build();
      cusc = new ErrorLoggingConcurrentUpdateSolrClient.Builder(
          ((Http2SolrClient) clients.get(clientIndex)).getBaseURL(), http2Client)
          .withQueueSize(30)
          .withThreadCount(3)
          .build();
    }
  }
  
  @Override
  public void safeStop() {
    stop = true;
    try {
      cusc.blockUntilFinished();
    } catch (IOException e) {
      log.warn("Exception waiting for the indexing client to finish", e);
    } finally {
      cusc.shutdownNow();
    }

  }

  @Override
  public int getFailCount() {
    return fails.get();
  }
  
  @Override
  public Set<String> getAddFails() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Set<String> getDeleteFails() {
    throw new UnsupportedOperationException();
  }
  
  static class ErrorLoggingConcurrentUpdateSolrClient extends ConcurrentUpdateHttp2SolrClient {
    public ErrorLoggingConcurrentUpdateSolrClient(Builder builder) {
      super(builder);
    }

    @Override
    public void handleError(Throwable ex) {
      log.warn("cusc error", ex);
    }

    static class Builder extends ConcurrentUpdateHttp2SolrClient.Builder {

      public Builder(String baseSolrUrl, Http2SolrClient client) {
        super(baseSolrUrl, client);
      }

      public ErrorLoggingConcurrentUpdateSolrClient build() {
        return new ErrorLoggingConcurrentUpdateSolrClient(this);
      }
    }
  }
  
}

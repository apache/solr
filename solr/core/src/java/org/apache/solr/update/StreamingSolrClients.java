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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient;
import org.apache.solr.client.solrj.jetty.ConcurrentUpdateJettySolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.update.SolrCmdDistributor.SolrError;
import org.eclipse.jetty.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingSolrClients {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final int runnerCount = Integer.getInteger("solr.cloud.replication.runners", 1);
  // should be less than solr.jetty.http.idleTimeout
  private final int pollQueueTimeMillis =
      Integer.getInteger("solr.cloud.client.pollQueueTime", 10000);

  private HttpJettySolrClient httpClient;

  private Map<String, ConcurrentUpdateBaseSolrClient> solrClients = new HashMap<>();
  private List<SolrError> errors = Collections.synchronizedList(new ArrayList<>());

  // Maps each UpdateRequest handed to a streaming client back to the distributor request it
  // belongs to, so a stream's outcome can be attributed to the requests that were in it. An
  // entry is removed when its stream completes, on failure as its error is reported and on
  // success as its result is tracked, so no request outlives its stream.
  private final Map<UpdateRequest, SolrCmdDistributor.Req> reqByUpdateRequest =
      Collections.synchronizedMap(new IdentityHashMap<>());

  private ExecutorService updateExecutor;

  public StreamingSolrClients(UpdateShardHandler updateShardHandler) {
    this.updateExecutor = updateShardHandler.getUpdateExecutor();
    this.httpClient = updateShardHandler.getUpdateOnlyHttpClient();
  }

  public List<SolrError> getErrors() {
    return errors;
  }

  public void clearErrors() {
    errors.clear();
  }

  public synchronized SolrClient getSolrClient(final SolrCmdDistributor.Req req) {
    reqByUpdateRequest.put(req.uReq, req);
    String url = getFullUrl(req.node.getUrl());
    ConcurrentUpdateBaseSolrClient client = solrClients.get(url);
    if (client == null) {
      // NOTE: increasing to more than 1 threadCount for the client could cause updates to be
      // reordered on a greater scale since the current behavior is to only increase the number of
      // connections/Runners when the queue is more than half full.
      final var defaultCore =
          StrUtils.isNotBlank(req.node.getCoreName()) ? req.node.getCoreName() : null;
      client =
          new ErrorReportingConcurrentUpdateSolrClient.Builder(
                  req.node.getBaseUrl(), httpClient, req, errors, reqByUpdateRequest)
              .withDefaultCollection(defaultCore)
              .withQueueSize(100)
              .withThreadCount(runnerCount)
              .withExecutorService(updateExecutor)
              .alwaysStreamDeletes()
              .setPollQueueTime(
                  pollQueueTimeMillis, TimeUnit.MILLISECONDS) // minimize connections created
              .build();

      solrClients.put(url, client);
    }

    return client;
  }

  public synchronized void blockUntilFinished() throws IOException {
    for (ConcurrentUpdateBaseSolrClient client : solrClients.values()) {
      client.blockUntilFinished();
    }
  }

  public synchronized void shutdown() {
    for (ConcurrentUpdateBaseSolrClient client : solrClients.values()) {
      client.close();
    }
  }

  private String getFullUrl(String url) {
    String fullUrl;
    if (!url.startsWith("http://") && !url.startsWith("https://")) {
      fullUrl = "http://" + url;
    } else {
      fullUrl = url;
    }
    return fullUrl;
  }

  public HttpJettySolrClient getHttpClient() {
    return httpClient;
  }

  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }
}

class ErrorReportingConcurrentUpdateSolrClient extends ConcurrentUpdateJettySolrClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrCmdDistributor.Req req;
  private final List<SolrError> errors;
  private final Map<UpdateRequest, SolrCmdDistributor.Req> reqByUpdateRequest;

  public ErrorReportingConcurrentUpdateSolrClient(Builder builder) {
    super(builder);
    this.req = builder.req;
    this.errors = builder.errors;
    this.reqByUpdateRequest = builder.reqByUpdateRequest;
  }

  @Override
  public void handleError(Throwable ex) {
    reqByUpdateRequest.remove(req.uReq);
    recordError(ex, req);
  }

  @Override
  protected void handleError(
      Throwable ex, List<UpdateRequest> requests, List<String> failedIds, String collection) {
    if (requests.isEmpty()) {
      handleError(ex);
      return;
    }
    // The failure belongs to the stream as a whole, so record it once against each request
    // that was actually in the stream, instead of once against the request this client
    // happened to be built with.
    for (UpdateRequest updateRequest : requests) {
      SolrCmdDistributor.Req streamReq = reqByUpdateRequest.remove(updateRequest);
      recordError(ex, streamReq != null ? streamReq : req);
    }
  }

  private void recordError(Throwable ex, SolrCmdDistributor.Req errorReq) {
    log.error("Error when calling {} to {}", errorReq, errorReq.node.getUrl(), ex);
    SolrError error = new SolrError();
    error.e = (Exception) ex;
    if (ex instanceof SolrException) {
      error.statusCode = ((SolrException) ex).code();
    }
    error.req = errorReq;
    errors.add(error);
    if (!errorReq.shouldRetry(error)) {
      // only track the error if we are not retrying the request
      errorReq.trackRequestResult(null, null, false);
    }
  }

  @Override
  public void onSuccess(Object responseMetadata, InputStream respBody) {
    Response jettyResponse = (Response) responseMetadata;
    req.trackRequestResult(jettyResponse, respBody, true);
  }

  @Override
  public void onSuccess(
      Object responseMetadata, InputStream respBody, List<UpdateRequest> requests) {
    // The stream succeeded, so its requests need no error attribution; drop their registry
    // entries rather than keeping the requests, and the documents they carry, reachable until
    // the update request that submitted them ends.
    for (UpdateRequest updateRequest : requests) {
      reqByUpdateRequest.remove(updateRequest);
    }
    onSuccess(responseMetadata, respBody);
  }

  static class Builder extends ConcurrentUpdateJettySolrClient.Builder {
    protected SolrCmdDistributor.Req req;
    protected List<SolrError> errors;
    protected Map<UpdateRequest, SolrCmdDistributor.Req> reqByUpdateRequest;

    /**
     * @param baseSolrUrl the base URL of a Solr node. Should <em>not</em> contain a collection or
     *     core name
     * @param client the client to use in making requests
     * @param req the command distributor request object for this client
     * @param errors a collector for any errors
     * @param reqByUpdateRequest maps each submitted update request back to its distributor request,
     *     for attributing stream errors
     */
    public Builder(
        String baseSolrUrl,
        HttpJettySolrClient client,
        SolrCmdDistributor.Req req,
        List<SolrError> errors,
        Map<UpdateRequest, SolrCmdDistributor.Req> reqByUpdateRequest) {
      super(baseSolrUrl, client);
      this.req = req;
      this.errors = errors;
      this.reqByUpdateRequest = reqByUpdateRequest;
    }

    @Override
    public ErrorReportingConcurrentUpdateSolrClient build() {
      return new ErrorReportingConcurrentUpdateSolrClient(this);
    }
  }
}

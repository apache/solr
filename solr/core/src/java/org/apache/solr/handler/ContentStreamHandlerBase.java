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
package org.apache.solr.handler;

import static org.apache.solr.common.params.CommonParams.FAILURE;
import static org.apache.solr.common.params.CommonParams.STATUS;

import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shares common code between various handlers that manipulate {@link
 * org.apache.solr.common.util.ContentStream} objects.
 */
public abstract class ContentStreamHandlerBase extends RequestHandlerBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void init(NamedList<?> args) {
    super.init(args);

    // Caching off by default
    httpCaching = false;
    if (args != null) {
      Object caching = args.get("httpCaching");
      if (caching != null) {
        httpCaching = Boolean.parseBoolean(caching.toString());
      }
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (checkCircuitBreakers(req, rsp)) {
      return; // Circuit breaker tripped, return immediately
    }

    /*
       We track update requests so that we can preserve consistency by waiting for them to complete
       on a node shutdown and then immediately trigger a leader election without waiting for the core to close.
       See how the SolrCoreState#pauseUpdatesAndAwaitInflightRequests() method is used in CoreContainer#shutdown()

       Also see https://issues.apache.org/jira/browse/SOLR-14942 for details on why we do not care for
       other kinds of requests.
    */
    SolrCoreState solrCoreState = req.getCore().getSolrCoreState();
    if (!solrCoreState.registerInFlightUpdate()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Updates are temporarily paused for core: " + req.getCore().getName());
    }
    try {
      SolrParams params = req.getParams();
      UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessorChain(params);

      UpdateRequestProcessor processor = processorChain.createProcessor(req, rsp);

      try {
        ContentStreamLoader documentLoader = newLoader(req, processor);

        Iterable<ContentStream> streams = req.getContentStreams();
        if (streams == null) {
          if (!RequestHandlerUtils.handleCommit(req, processor, params, false)
              && !RequestHandlerUtils.handleRollback(req, processor, params, false)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing content stream");
          }
        } else {

          for (ContentStream stream : streams) {
            documentLoader.load(req, rsp, stream, processor);
          }

          // Perhaps commit from the parameters
          RequestHandlerUtils.handleCommit(req, processor, params, false);
          RequestHandlerUtils.handleRollback(req, processor, params, false);
        }
      } finally {
        // finish the request
        try {
          processor.finish();
        } finally {
          processor.close();
        }
      }
    } finally {
      solrCoreState.deregisterInFlightUpdate();
    }
  }

  /**
   * Check if {@link SolrRequestType#UPDATE} circuit breakers are tripped. Override this method in
   * sub classes that do not want to check circuit breakers.
   *
   * @return true if circuit breakers are tripped, false otherwise.
   */
  protected boolean checkCircuitBreakers(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (isInternalShardRequest(req)) {
      if (log.isTraceEnabled()) {
        log.trace("Internal request, skipping circuit breaker check");
      }
      return false;
    }
    CircuitBreakerRegistry circuitBreakerRegistry = req.getCore().getCircuitBreakerRegistry();
    if (circuitBreakerRegistry.isEnabled(SolrRequestType.UPDATE)) {
      List<CircuitBreaker> trippedCircuitBreakers =
          circuitBreakerRegistry.checkTripped(SolrRequestType.UPDATE);
      if (trippedCircuitBreakers != null) {
        String errorMessage = CircuitBreakerRegistry.toErrorMessage(trippedCircuitBreakers);
        rsp.add(STATUS, FAILURE);
        rsp.setException(
            new SolrException(
                CircuitBreaker.getErrorCode(trippedCircuitBreakers),
                "Circuit Breakers tripped " + errorMessage));
        return true;
      }
    }
    return false;
  }

  protected abstract ContentStreamLoader newLoader(
      SolrQueryRequest req, UpdateRequestProcessor processor);
}

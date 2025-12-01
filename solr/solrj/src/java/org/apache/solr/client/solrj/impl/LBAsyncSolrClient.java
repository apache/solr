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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.MDC;

/** A {@link LBSolrClient} adding {@link #requestAsync(Req)}. */
public abstract class LBAsyncSolrClient extends LBSolrClient {
  // formerly known as LBHttp2SolrClient, using Http2SolrClient (jetty)

  protected final HttpSolrClientBase solrClient;

  protected LBAsyncSolrClient(Builder<?> builder) {
    super(builder);
    this.solrClient = builder.getSolrClient();
  }

  @Override
  protected HttpSolrClientBase getClient(Endpoint endpoint) {
    return solrClient;
  }

  /**
   * Execute an asynchronous request against one or more hosts for a given collection. The passed-in
   * Req object includes a List of Endpoints. This method always begins with the first Endpoint in
   * the list and if unsuccessful tries each in turn until the request is successful. Consequently,
   * this method does not actually Load Balance. It is up to the caller to shuffle the List of
   * Endpoints if Load Balancing is desired.
   *
   * @param req the wrapped request to perform
   * @return a {@link CompletableFuture} that tracks the progress of the async request.
   */
  public CompletableFuture<Rsp> requestAsync(Req req) {
    CompletableFuture<Rsp> apiFuture = new CompletableFuture<>();
    Rsp rsp = new Rsp();
    boolean isAdmin =
        req.request.getRequestType() == SolrRequestType.ADMIN && !req.request.requiresCollection();
    boolean isNonRetryable = req.request.getRequestType() == SolrRequestType.UPDATE || isAdmin;
    EndpointIterator it = new EndpointIterator(req, zombieServers);
    AtomicReference<CompletableFuture<NamedList<Object>>> currentFuture = new AtomicReference<>();
    RetryListener retryListener =
        new RetryListener() {

          @Override
          public void onSuccess(Rsp rsp) {
            apiFuture.complete(rsp);
          }

          @Override
          public void onFailure(Exception e, boolean retryReq) {
            if (retryReq) {
              Endpoint url;
              try {
                url = it.nextOrError(e);
              } catch (Throwable ex) {
                apiFuture.completeExceptionally(e);
                return;
              }
              MDC.put("LBSolrClient.url", url.toString());
              if (!apiFuture.isCancelled()) {
                try {
                  CompletableFuture<NamedList<Object>> future =
                      doAsyncRequest(
                          url, req, rsp, isNonRetryable, it.isServingZombieServer(), this);
                  currentFuture.set(future);
                } catch (Throwable ex) {
                  apiFuture.completeExceptionally(ex);
                }
              }
            } else {
              apiFuture.completeExceptionally(e);
            }
          }
        };
    try {
      CompletableFuture<NamedList<Object>> future =
          doAsyncRequest(
              it.nextOrError(),
              req,
              rsp,
              isNonRetryable,
              it.isServingZombieServer(),
              retryListener);
      currentFuture.set(future);
    } catch (SolrServerException e) {
      apiFuture.completeExceptionally(e);
      return apiFuture;
    }
    apiFuture.exceptionally(
        (error) -> {
          if (apiFuture.isCancelled()) {
            currentFuture.get().cancel(true);
          }
          return null;
        });
    return apiFuture;
  }

  private interface RetryListener {
    void onSuccess(Rsp rsp);

    void onFailure(Exception e, boolean retryReq);
  }

  private CompletableFuture<NamedList<Object>> doAsyncRequest(
      Endpoint endpoint,
      Req req,
      Rsp rsp,
      boolean isNonRetryable,
      boolean isZombie,
      RetryListener listener) {
    String baseUrl = endpoint.toString();
    rsp.server = baseUrl;
    try {
      CompletableFuture<NamedList<Object>> future =
          requestAsyncWithUrl(getClient(endpoint), baseUrl, req.getRequest());
      future.whenComplete(
          (result, throwable) -> {
            if (!future.isCompletedExceptionally()) {
              onSuccessfulRequest(result, endpoint, rsp, isZombie, listener);
            } else if (!future.isCancelled()) {
              onFailedRequest(throwable, endpoint, isNonRetryable, isZombie, listener);
            }
          });
      return future;
    } catch (SolrServerException | IOException e) {
      // Unreachable, since 'requestAsyncWithUrl' above is running the request asynchronously
      throw new RuntimeException(e);
    }
  }

  protected abstract CompletableFuture<NamedList<Object>> requestAsyncWithUrl(
      SolrClient client, String baseUrl, SolrRequest<?> request)
      throws SolrServerException, IOException;

  private void onSuccessfulRequest(
      NamedList<Object> result,
      Endpoint endpoint,
      Rsp rsp,
      boolean isZombie,
      RetryListener listener) {
    rsp.rsp = result;
    if (isZombie) {
      reviveZombieServer(endpoint);
    }
    listener.onSuccess(rsp);
  }

  private void onFailedRequest(
      Throwable oe,
      Endpoint endpoint,
      boolean isNonRetryable,
      boolean isZombie,
      RetryListener listener) {
    try {
      throw (Exception) oe;
    } catch (SolrException e) {
      if (!isNonRetryable && e instanceof RemoteSolrException rse) {
        isNonRetryable = rse.shouldSkipRetry();
      }
      // we retry on 404 or 403 or 503 or 500
      // unless it's an update - then we only retry on connect exception
      if (!isNonRetryable && RETRY_CODES.contains(e.code())) {
        listener.onFailure((!isZombie) ? makeServerAZombie(endpoint, e) : e, true);
      } else {
        // Server is alive but the request was likely malformed or invalid
        if (isZombie) {
          reviveZombieServer(endpoint);
        }
        listener.onFailure(e, false);
      }
    } catch (SocketException e) {
      if (!isNonRetryable || e instanceof ConnectException) {
        listener.onFailure((!isZombie) ? makeServerAZombie(endpoint, e) : e, true);
      } else {
        listener.onFailure(e, false);
      }
    } catch (SocketTimeoutException e) {
      if (!isNonRetryable) {
        listener.onFailure((!isZombie) ? makeServerAZombie(endpoint, e) : e, true);
      } else {
        listener.onFailure(e, false);
      }
    } catch (SolrServerException e) {
      Throwable rootCause = e.getRootCause();
      if (!isNonRetryable && rootCause instanceof IOException) {
        listener.onFailure((!isZombie) ? makeServerAZombie(endpoint, e) : e, true);
      } else if (isNonRetryable && rootCause instanceof ConnectException) {
        listener.onFailure((!isZombie) ? makeServerAZombie(endpoint, e) : e, true);
      } else {
        listener.onFailure(e, false);
      }
    } catch (Throwable e) {
      listener.onFailure(new SolrServerException(e), false);
    }
  }
}

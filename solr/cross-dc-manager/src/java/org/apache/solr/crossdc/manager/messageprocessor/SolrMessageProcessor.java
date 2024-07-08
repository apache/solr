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
package org.apache.solr.crossdc.manager.messageprocessor;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.crossdc.common.CrossDcConstants;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.ResubmitBackoffPolicy;
import org.apache.solr.crossdc.common.SolrExceptionUtil;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Message processor implements all the logic to process a MirroredSolrRequest. It handles:
 *
 * <ul>
 *   <li>Sending the update request to Solr
 *   <li>Discarding or retrying failed requests
 *   <li>Flagging requests for resubmission by the underlying consumer implementation
 * </ul>
 */
public class SolrMessageProcessor extends MessageProcessor
    implements IQueueHandler<MirroredSolrRequest<?>> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final MetricRegistry metrics =
      SharedMetricRegistries.getOrCreate(Consumer.METRICS_REGISTRY);

  final CloudSolrClient client;

  private static final String VERSION_FIELD = "_version_";

  public SolrMessageProcessor(CloudSolrClient client, ResubmitBackoffPolicy resubmitBackoffPolicy) {
    super(resubmitBackoffPolicy);
    this.client = client;
  }

  @Override
  @SuppressWarnings("try")
  public Result<MirroredSolrRequest<?>> handleItem(MirroredSolrRequest<?> mirroredSolrRequest) {
    try (final MDC.MDCCloseable ignored =
        MDC.putCloseable("collection", getCollectionFromRequest(mirroredSolrRequest))) {
      connectToSolrIfNeeded();

      // TODO: isn't this handled by the mirroring handler?

      // preventCircularMirroring(mirroredSolrRequest);

      return processMirroredRequest(mirroredSolrRequest);
    }
  }

  private Result<MirroredSolrRequest<?>> processMirroredRequest(MirroredSolrRequest<?> request) {
    final Result<MirroredSolrRequest<?>> result = handleSolrRequest(request);
    // Back-off before returning
    backoffIfNeeded(result, request.getType());
    return result;
  }

  private Result<MirroredSolrRequest<?>> handleSolrRequest(
      MirroredSolrRequest<?> mirroredSolrRequest) {

    SolrRequest<?> request = mirroredSolrRequest.getSolrRequest();
    final SolrParams requestParams = request.getParams();

    if (log.isDebugEnabled()) {
      log.debug("handleSolrRequest start params={}", requestParams);
    }

    // TODO: isn't this handled by the mirroring handler?

    //  final String shouldMirror = requestParams.get("shouldMirror");
    //
    //  if ("false".equalsIgnoreCase(shouldMirror)) {
    //    log.warn("Skipping mirrored request because shouldMirror is set to false.
    // request={}", requestParams);
    //    return new Result<>(ResultStatus.FAILED_NO_RETRY);
    //  }
    logFirstAttemptLatency(mirroredSolrRequest);

    Result<MirroredSolrRequest<?>> result;
    try {
      prepareIfUpdateRequest(request);
      logRequest(request);
      result = processMirroredSolrRequest(request, mirroredSolrRequest.getType());
    } catch (Exception e) {
      result = handleException(mirroredSolrRequest, e);
    }
    if (log.isDebugEnabled()) {
      log.debug("handleSolrRequest end params={} result={}", requestParams, result);
    }
    return result;
  }

  private Result<MirroredSolrRequest<?>> handleException(
      MirroredSolrRequest<?> mirroredSolrRequest, Exception e) {
    final SolrException solrException = SolrExceptionUtil.asSolrException(e);
    logIf4xxException(solrException);
    if (!isRetryable(e)) {
      log.error("Non retryable exception processing Solr update", e);
      return new Result<>(ResultStatus.FAILED_NO_RETRY, e);
    } else {
      logFailure(mirroredSolrRequest, e, solrException);
      mirroredSolrRequest.setAttempt(mirroredSolrRequest.getAttempt() + 1);
      maybeBackoff(mirroredSolrRequest, solrException);
      return new Result<>(ResultStatus.FAILED_RESUBMIT, e, mirroredSolrRequest);
    }
  }

  private void maybeBackoff(MirroredSolrRequest<?> request, SolrException solrException) {
    if (solrException == null) {
      return;
    }
    long sleepTimeMs = 1000;
    String backoffTimeSuggested = solrException.getMetadata("backoffTime-ms");
    if (backoffTimeSuggested != null && !"0".equals(backoffTimeSuggested)) {
      // If backoff policy is not configured (returns "0" by default), then sleep 1 second.
      // If configured, do as it says.
      sleepTimeMs = Math.max(1, Long.parseLong(backoffTimeSuggested));
    }
    log.info("Consumer backoff. sleepTimeMs={}", sleepTimeMs);
    metrics.meter(MetricRegistry.name(request.getType().name(), "backoff")).mark(sleepTimeMs);
    uncheckedSleep(sleepTimeMs);
  }

  private boolean isRetryable(Exception e) {
    SolrException se = SolrExceptionUtil.asSolrException(e);

    if (se != null) {
      int code = se.code();
      if (code == SolrException.ErrorCode.CONFLICT.code) {
        return false;
      }
    }
    // Everything other than version conflict exceptions should be retried.
    log.warn("Unexpected exception, will resubmit the request to the queue", e);
    return true;
  }

  private void logIf4xxException(SolrException solrException) {
    // This shouldn't really happen but if it doesn, it most likely requires fixing in the return
    // code from Solr.
    if (solrException != null && 400 <= solrException.code() && solrException.code() < 500) {
      log.error("Exception occurred with 4xx response. {}", solrException.code(), solrException);
    }
  }

  private void logFailure(
      MirroredSolrRequest<?> mirroredSolrRequest, Exception e, SolrException solrException) {
    // This shouldn't really happen.
    if (solrException != null && 400 <= solrException.code() && solrException.code() < 500) {
      log.error("Exception occurred with 4xx response. {}", solrException.code(), solrException);
      return;
    }

    log.warn(
        "Resubmitting mirrored solr request after failure errorCode={} retryCount={}",
        solrException != null ? solrException.code() : -1,
        mirroredSolrRequest.getAttempt(),
        e);
  }

  /** Process the SolrRequest. If not, this method throws an exception. */
  private Result<MirroredSolrRequest<?>> processMirroredSolrRequest(
      SolrRequest<?> request, MirroredSolrRequest.Type type) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug(
          "Sending request to Solr at ZK address={} with params {}",
          ZkStateReader.from(client).getZkClient().getZkServerAddress(),
          request.getParams());
    }
    Result<MirroredSolrRequest<?>> result;
    SolrResponseBase response;
    Timer.Context ctx = metrics.timer(MetricRegistry.name(type.name(), "outputTime")).time();
    try {
      response = (SolrResponseBase) request.process(client);
    } finally {
      ctx.stop();
    }

    int status = response.getStatus();

    if (log.isTraceEnabled()) {
      log.trace("result status={}", status);
    }

    if (status != 0) {
      metrics.counter(MetricRegistry.name(type.name(), "outputErrors")).inc();
      throw new SolrException(SolrException.ErrorCode.getErrorCode(status), "response=" + response);
    }

    if (log.isDebugEnabled()) {
      log.debug(
          "Finished sending request to Solr at ZK address={} with params {} status_code={}",
          ZkStateReader.from(client).getZkClient().getZkServerAddress(),
          request.getParams(),
          status);
    }
    result = new Result<>(ResultStatus.HANDLED);
    return result;
  }

  private void logRequest(SolrRequest<?> request) {
    if (request instanceof UpdateRequest) {
      final StringBuilder rmsg = new StringBuilder(64);
      String collection = request.getCollection();
      rmsg.append("Submitting update request for collection=")
          .append(collection != null ? collection : request.getParams().get("collection"));
      if (((UpdateRequest) request).getDeleteById() != null) {
        final int numDeleteByIds = ((UpdateRequest) request).getDeleteById().size();
        rmsg.append(" numDeleteByIds=").append(numDeleteByIds);
      }
      if (((UpdateRequest) request).getDocuments() != null) {
        final int numUpdates = ((UpdateRequest) request).getDocuments().size();
        rmsg.append(" numUpdates=").append(numUpdates);
      }
      if (((UpdateRequest) request).getDeleteQuery() != null) {
        final int numDeleteByQuery = ((UpdateRequest) request).getDeleteQuery().size();
        rmsg.append(" numDeleteByQuery=").append(numDeleteByQuery);
      }
      if (log.isInfoEnabled()) {
        log.info(rmsg.toString());
      }
    }
  }

  /**
   * Clean up the Solr request to be submitted locally.
   *
   * @param request The SolrRequest to be cleaned up for submitting locally.
   */
  private void prepareIfUpdateRequest(SolrRequest<?> request) {
    if (request instanceof UpdateRequest) {
      // Remove versions from add requests
      UpdateRequest updateRequest = (UpdateRequest) request;

      List<SolrInputDocument> documents = updateRequest.getDocuments();
      if (log.isTraceEnabled()) {
        log.trace(
            "update request docs={} deletebyid={} deletebyquery={}",
            documents,
            updateRequest.getDeleteById(),
            updateRequest.getDeleteQuery());
      }
      if (documents != null) {
        for (SolrInputDocument doc : documents) {
          sanitizeDocument(doc);
        }
      }
      removeVersionFromDeleteByIds(updateRequest);
    }
  }

  /** Strips fields that are problematic for replication. */
  private void sanitizeDocument(SolrInputDocument doc) {
    SolrInputField field = doc.getField(VERSION_FIELD);
    if (log.isTraceEnabled()) {
      log.trace("Removing {} value={}", VERSION_FIELD, field == null ? "null" : field.getValue());
    }
    doc.remove(VERSION_FIELD);
  }

  private void removeVersionFromDeleteByIds(UpdateRequest updateRequest) {
    if (log.isTraceEnabled()) {
      log.trace("remove versions from deletebyids");
    }
    Map<String, Map<String, Object>> deleteIds = updateRequest.getDeleteByIdMap();
    if (deleteIds != null) {
      for (Map<String, Object> idParams : deleteIds.values()) {
        if (idParams != null) {
          idParams.put(UpdateRequest.VER, null);
        }
      }
    }
  }

  private void logFirstAttemptLatency(MirroredSolrRequest<?> mirroredSolrRequest) {
    // Only record the latency of the first attempt, measuring the latency from
    // submitting on the primary side until the request is eligible to be consumed on the buddy side
    // (or vice versa).
    if (mirroredSolrRequest.getAttempt() == 1) {
      final long latency = System.nanoTime() - mirroredSolrRequest.getSubmitTimeNanos();
      log.debug("First attempt latency = {} ns", latency);
      metrics
          .timer(MetricRegistry.name(mirroredSolrRequest.getType().name(), "outputLatency"))
          .update(latency, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * Adds {@link CrossDcConstants#SHOULD_MIRROR}=false to the params if it's not already specified.
   * Logs a warning if it is specified and NOT set to false. (i.e. circular mirror may occur)
   *
   * @param mirroredSolrRequest MirroredSolrRequest object that is being processed.
   */
  void preventCircularMirroring(MirroredSolrRequest<?> mirroredSolrRequest) {
    if (mirroredSolrRequest.getSolrRequest() instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) mirroredSolrRequest.getSolrRequest();
      ModifiableSolrParams params = updateRequest.getParams();
      String shouldMirror = (params == null ? null : params.get(CrossDcConstants.SHOULD_MIRROR));
      if (shouldMirror == null) {
        log.warn(
            "{} param is missing - setting to false. Request={}",
            CrossDcConstants.SHOULD_MIRROR,
            mirroredSolrRequest);
        updateRequest.setParam(CrossDcConstants.SHOULD_MIRROR, "false");
      } else if (!"false".equalsIgnoreCase(shouldMirror)) {
        log.warn("{} param equal to {}", CrossDcConstants.SHOULD_MIRROR, shouldMirror);
      }
    } else {
      SolrParams params = mirroredSolrRequest.getSolrRequest().getParams();
      String shouldMirror = (params == null ? null : params.get(CrossDcConstants.SHOULD_MIRROR));
      if (shouldMirror == null) {
        if (params instanceof ModifiableSolrParams) {
          log.warn("{} param is missing - setting to false", CrossDcConstants.SHOULD_MIRROR);
          ((ModifiableSolrParams) params).set(CrossDcConstants.SHOULD_MIRROR, "false");
        } else {
          log.warn(
              "{} param is missing and params are not modifiable", CrossDcConstants.SHOULD_MIRROR);
        }
      } else if (!"false".equalsIgnoreCase(shouldMirror)) {
        log.warn("{} param is present and set to {}", CrossDcConstants.SHOULD_MIRROR, shouldMirror);
      }
    }
  }

  private void connectToSolrIfNeeded() {
    // Don't try to consume anything if we can't connect to the solr server
    boolean connected = false;
    while (!connected) {
      try {
        client.connect(); // volatile null-check if already connected
        connected = true;
      } catch (Exception e) {
        log.error("Unable to connect to solr server. Not consuming.", e);
        uncheckedSleep(5000);
      }
    }
  }

  public void uncheckedSleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void backoffIfNeeded(
      Result<MirroredSolrRequest<?>> result, MirroredSolrRequest.Type type) {
    if (result.status().equals(ResultStatus.FAILED_RESUBMIT)) {
      final long backoffMs = getResubmitBackoffPolicy().getBackoffTimeMs(result.getItem());
      if (backoffMs > 0L) {
        metrics.meter(MetricRegistry.name(type.name(), "backoff")).mark(backoffMs);
        try {
          Thread.sleep(backoffMs);
        } catch (final InterruptedException ex) {
          // we're about to exit the method anyway, so just log this and return the item. Let the
          // caller handle it.
          log.warn("Thread interrupted while backing off before retry");
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private static String getCollectionFromRequest(MirroredSolrRequest<?> mirroredSolrRequest) {
    if (mirroredSolrRequest == null || mirroredSolrRequest.getSolrRequest() == null) return null;

    return mirroredSolrRequest.getSolrRequest().getCollection();
  }
}

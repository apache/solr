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

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.solr.util.tracing.SolrRequestCarrier;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.AsyncTracker;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.LeaderRequestReplicationTracker;
import org.apache.solr.update.processor.DistributedUpdateProcessor.RollupRequestReplicationTracker;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Used for distributing commands from a shard leader to its replicas.
 */
public class SolrCmdDistributor implements Closeable {
  private static final int MAX_RETRIES_ON_FORWARD = 1;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ConnectionManager.IsClosed isClosed;
  private final CoreDescriptor coreDesc;
  private final ZkShardTerms zkShardTerms;

  private final Set<Cancellable> asyncRequests = Collections.newSetFromMap(new NonBlockingHashMap<>());

  private volatile boolean finished = false; // see finish()

  private static final int maxRetries = MAX_RETRIES_ON_FORWARD;

  // Keyed by the Req (the per-(command,node) forward), NOT by the UpdateCommand. A single command is
  // forwarded to multiple replicas, so multiple replicas can fail the same command; keying by command
  // collapsed those failures to one (last writer wins), which meant only one of several partitioned
  // followers got reported / pushed into a lower term and recovery. The Req is unique per (command,
  // node) and a retry reuses the same Req, so this keeps per-node failures distinct while still
  // de-duplicating retries of the same forward.
  private final Map<Object, Error> allErrors = new NonBlockingHashMap<>();
  
  private final Http2SolrClient requestSolrClient;

  AsyncTracker asyncTracker = new AsyncTracker(-1, false, 0);

 // private final Http2SolrClient replicaSolrClient;

  private final static long TIMEOUT = 1;

  private volatile boolean closed;

  private volatile Throwable cancelExeption;

  public SolrCmdDistributor(UpdateShardHandler updateShardHandler) {
    assert ObjectReleaseTracker.getInstance().track(this);
    coreDesc = null;
    zkShardTerms = null;
    this.requestSolrClient = updateShardHandler.getSolrCmdDistributorClient();
    //this.replicaSolrClient = new Http2SolrClient.Builder().withHttpClient(updateShardHandler.getTheSharedHttpClient()).markInternalRequest().build();
    isClosed = null;
  }

  public SolrCmdDistributor(UpdateShardHandler updateShardHandler, ConnectionManager.IsClosed isClosed, CoreDescriptor coreDesc, ZkShardTerms zkShardTerms) {
    //assert ObjectReleaseTracker.track(this);
    this.zkShardTerms = zkShardTerms;
    this.coreDesc = coreDesc;
    this.requestSolrClient = updateShardHandler.getSolrCmdDistributorClient();
    //this.replicaSolrClient = new Http2SolrClient.Builder().withHttpClient(updateShardHandler.getTheSharedHttpClient()).markInternalRequest().build();
    this.isClosed = isClosed;
  }

  public void finish() {
    assert !finished : "lifecycle sanity check";

    if (isClosed == null || !isClosed.isClosed()) {
      try {
        asyncTracker.waitForComplete(10, TimeUnit.MINUTES);
      } catch (TimeoutException timeoutException) {
        log.warn("Timeout waiting for requests to finish");
      }
    } else {
      //cancels.forEach(cancellable -> cancellable.cancel());
      Error error = new Error("ac");
      error.t = new AlreadyClosedException();
      AlreadyClosedUpdateCmd cmd = new AlreadyClosedUpdateCmd(null);
      allErrors.put(cmd, error);
      cancelAll();
    }
    finished = true;
  }
  
  public void close() {
    this.closed = true;
    cancelAll();
    asyncTracker.close();
    assert ObjectReleaseTracker.getInstance().release(this);
  }

  private void cancelAll() {
    asyncRequests.forEach(cancellable -> cancellable.cancel());
  }

  public boolean checkRetry(Error err) {
    String oldNodeUrl = err.req.node.getUrl();

    // if there is a retry url, we want to retry...
    boolean isRetry = err.req.node.checkRetry();

    boolean doRetry = false;
    int rspCode = err.statusCode;

    if (testing_errorHook != null) Diagnostics.call(testing_errorHook,
            err.t);

    // this can happen in certain situations such as close
    if (isRetry) {
      // if it's a io exception exception, lets try again
      if (err.t instanceof SolrServerException) {
        if (((SolrServerException) err.t).getRootCause() instanceof IOException) {
          doRetry = true;
        }
      }

//      if (err.t instanceof IOException && !(err.t instanceof ClosedChannelException)) {
//        doRetry = true;
//      }

      if (err.req != null && err.req.retries.get() < maxRetries && doRetry && (isClosed == null || !isClosed.isClosed())) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {

        }
        err.req.retries.incrementAndGet();

        SolrException.log(SolrCmdDistributor.log, "sending update to "
                + oldNodeUrl + " failed - retrying ... retries: "
                + err.req.retries + " " + err.req.cmd.toString() + " params:"
                + err.req.uReq.getParams() + " rsp:" + rspCode, err.t);
        if (log.isDebugEnabled()) log.debug("check retry true");
        return true;
      } else {
        log.info("max retries exhausted or not a retryable error {} {}", err.req.retries, rspCode);
        return false;
      }
    } else {
      log.info("not a retry request, retry false");
      return false;
    }

  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    distribDelete(cmd, nodes, params, false, null, null);
  }

  public void distribDelete(DeleteUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean sync,
                            RollupRequestReplicationTracker rollupTracker,
                            LeaderRequestReplicationTracker leaderTracker) {

    if (cancelExeption != null) {
      Throwable exp = cancelExeption;
      cancelExeption = null;
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exp);
    }

    if (nodes == null) return;

    for (Node node : nodes) {
      if (node == null) continue;
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);
      uReq.setCommitWithin(cmd.commitWithin);
      if (cmd.isDeleteById()) {
        uReq.deleteById(cmd.getId(), cmd.getRoute(), cmd.getVersion());
        submit(new Req(cmd, node, uReq, sync, false, rollupTracker, leaderTracker), "del");
      } else {
        uReq.deleteByQuery(cmd.query);
        submit(new Req(cmd, node, uReq, sync, true, rollupTracker, leaderTracker), "del");
      }

    }
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    distribAdd(cmd, nodes, params, false, null, null);
  }

  public void  distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean synchronous) throws IOException {
    distribAdd(cmd, nodes, params, synchronous, null, null);
  }

  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean synchronous,
                         RollupRequestReplicationTracker rollupTracker,
                         LeaderRequestReplicationTracker leaderTracker) throws IOException {
    if (cancelExeption != null) {
      Throwable exp = cancelExeption;
      cancelExeption = null;
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exp);
    }
    if (nodes == null) return;
    for (Node node : nodes) {
      if (node == null) continue;
      UpdateRequest uReq = new UpdateRequest();
      if (cmd.isLastDocInBatch)
        uReq.lastDocInBatch();
      uReq.setParams(params);
      uReq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
      if (cmd.isInPlaceUpdate()) {
        params.set(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, String.valueOf(cmd.prevVersion));
      }
      submit(new Req(cmd, node, uReq, synchronous, false, rollupTracker, leaderTracker), "add");
    }
    
  }

  public void distribCommit(CommitUpdateCommand cmd, List<Node> nodes,
      ModifiableSolrParams params) {
    if (cancelExeption != null) {
      Throwable exp = cancelExeption;
      cancelExeption = null;
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exp);
    }
    if (log.isDebugEnabled()) {
      log.debug("Distrib commit to: {} params: {}", nodes, params);
    }

    for (Node node : nodes) {
      if (node == null) continue;
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);

      addCommit(uReq, cmd);
      Req req = new Req(cmd, node, uReq, false, true);
      req.isCommit = true;
      submit(req, "commit");
    }
  }

  public void blockAndDoRetries() {
    try {
      if (cancelExeption != null) {
        Throwable exp = cancelExeption;
        cancelExeption = null;
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exp);
      }
      asyncTracker.waitForComplete(TIMEOUT, TimeUnit.MINUTES);

      //replicaSolrClient.waitForOutstandingRequests(TIMEOUT, TimeUnit.MINUTES);
    } catch (TimeoutException timeoutException) {
      log.warn("Timeout waiting for requests to finish", timeoutException);
    }

  }
  
  static void addCommit(UpdateRequest ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher, cmd.maxOptimizeSegments, cmd.softCommit, cmd.expungeDeletes, cmd.openSearcher);
  }

  // section submit
  private void submit(final Req req, String tag) {

        if (cancelExeption != null) {
          Throwable exp = cancelExeption;
          cancelExeption = null;
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exp);
        }

    //    if (log.isDebugEnabled()) {
    //      log.debug("sending update to " + req.node.getUrl() + " retry:" + req.retries + " " + req.cmd + " params:" + req.uReq.getParams());
    //    }
    if (log.isDebugEnabled()) {
      if (req.cmd instanceof AddUpdateCommand) {
        log.info("sending update to {} retry:{} docid={} {} params:{}", req.node.getUrl(), req.retries, ((AddUpdateCommand) req.cmd).getPrintableId(), req.cmd,
            req.uReq.getParams());
      } else {
        log.info("sending update to {} retry:{} docid={} params:{}", req.node.getUrl(), req.retries, req.cmd, req.uReq.getParams());
      }
    }
    SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
    if (requestInfo != null) {
      HttpServletRequest httpReq = requestInfo.httpRequest;
      if (httpReq != null) {
        Enumeration<String> hnames = httpReq.getHeaderNames();

        while (hnames.hasMoreElements()) {
          String hname = hnames.nextElement();
          req.uReq.addHeader(hname, httpReq.getHeader(hname));
        }

      }
      requestInfo.getUserPrincipal();
      req.uReq.setUserPrincipal(requestInfo.getReq().getUserPrincipal());
    }

    req.uReq.setBasePath(req.node.getUrl());

    // Propagate the active OpenTracing span context onto the forwarded (leader->replica /
    // leader->subshard) update request so the receiving node's span is recorded as a child of this
    // span. Mirrors the injection HttpShardHandler performs for distributed query sub-requests;
    // without it forwarded-update spans have no parent (parentId=0).
    Tracer tracer = GlobalTracer.getTracer();
    Span span = tracer != null ? tracer.activeSpan() : null;
    if (span != null) {
      tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new SolrRequestCarrier(req.uReq));
    }

    if (req.synchronous) {
      blockAndDoRetries();
      asyncTracker.register();
      try {
        requestSolrClient.request(req.uReq);
      } catch (Exception e) {
        log.error("Exception sending synchronous dist update", e);
        Error error = new Error(tag);
        error.t = e;
        error.req = req;
        if (e instanceof SolrException) {
          error.statusCode = ((SolrException) e).code();
        }
        allErrors.put(req, error);
      } finally {
        asyncTracker.arrive();
      }

      return;
    }
    if (req.requiresWait) {
      blockAndDoRetries();
    }
    try {
      asyncTracker.register();
      AtomicReference<Cancellable> asyncReq = new AtomicReference<>();
      asyncReq.set(requestSolrClient.asyncRequest(req.uReq, null, new AsyncListener<>() {
        @Override public void onSuccess(NamedList result, int code, Object context) {
          if (log.isTraceEnabled()) {
            log.trace("Success for distrib update {}", result);
          }
          if (asyncReq.get() != null) {
            asyncRequests.remove(asyncReq.get());
          }
          // record the achieved replication factor for this forward (no-op unless the client
          // requested rf tracking, i.e. a leader/rollup tracker is present on the Req)
          req.trackRequestResult(result, true);
          asyncTracker.arrive();
        }

        @Override public void onFailure(Throwable t, int code, Object context) {
          try {
            if (asyncReq.get() != null) {
              asyncRequests.remove(asyncReq.get());
            }

            Error error = new Error(tag);
            error.t = t;
            error.req = req;
            error.statusCode = code;

            log.error("Exception sending dist update code={}", code, t);

            if (t instanceof ClosedChannelException) {
              // we may get success but be told the peer is shutting down via exception
              log.info("Channel closed code={}", code);


                allErrors.put(req, error);
                cancelExeption = new AlreadyClosedException();
                return;

            }


            if (t instanceof IOException) {
              if (t.getMessage() != null && t.getMessage().contains("cancel_stream_error")) {

                  allErrors.put(req, error);
                  cancelExeption = new AlreadyClosedException();
                  return;


              }
            }

            if (t instanceof Http2SolrClient.CancelledException) {
              log.info("Stream cancelled code={}", code);
              if (code != 200) {
                allErrors.put(req, error);
              } else {
                // FLT-INVESTIGATION (temporary; revert before commit): a cancelled forward reported
                // with code=200 is currently treated as success and NOT recorded as an error, so
                // doFinish never sees it, never bumps the target's shard term, and the replica silently
                // diverges (suspected source of MISSING docs on a follower under ChaosMonkey node-kills).
                log.error("FLT-DROP cancelled-as-success node={} docid={} code={} retries={} cmd={}",
                    req.node.getUrl(),
                    (req.cmd instanceof AddUpdateCommand ? ((AddUpdateCommand) req.cmd).getPrintableId() : "n/a"),
                    code, req.retries, req.cmd);
              }
              return;
            }

            // MRM TODO: - we want to prevent any more from this request
            // to go just to this node rather than stop the whole request
            if (code == 404) {
              // Record the 404 as an error so the failing replica's shard term is bumped and
              // leader-initiated recovery kicks in; otherwise the follower silently diverges
              // (the lost update is never re-sought, especially for the last/only doc in a batch).
              allErrors.put(req, error);
              cancelExeption = t;
              return;
            }



            boolean retry = checkRetry(error);

            if (retry) {
              log.info("Retrying distrib update on error: {}", t.getMessage());
              try {
                submit(req, tag);
              } catch (AlreadyClosedException e) {

              }
            } else {
              allErrors.put(req, error);
            }
          } finally {
            asyncTracker.arrive();
          }
        }
      }));
      asyncRequests.add(asyncReq.get());
      //      } else {
      //        replicaSolrClient.asyncRequest(req.uReq, null, new AsyncListener<>() {
      //          @Override public void onSuccess(NamedList result, int code) {
      //            if (log.isTraceEnabled()) {
      //              log.trace("Success for distrib update {}", result);
      //            }
      //          }
      //
      //          @Override public void onFailure(Throwable t, int code) {
      //            log.error("Exception sending dist update code={}", code, t);
      //
      //            if (code == ErrorCode.CANCEL_STREAM_ERROR.code) {
      //              log.info("Stream cancelled code={}", code);
      //              return;
      //            }
      //
      //            // MRM TODO: - we want to prevent any more from this request
      //            // to go just to this node rather than stop the whole request
      //            if (code == 404) {
      //              cancelExeption = t;
      //              return;
      //            }
      //
      //            Error error = new Error(tag);
      //            error.t = t;
      //            error.req = req;
      //            if (t instanceof SolrException) {
      //              error.statusCode = ((SolrException) t).code();
      //            }
      //
      //            boolean retry = checkRetry(error);
      //
      //            if (retry) {
      //              log.info("Retrying distrib update on error: {}", t.getMessage());
      //              try {
      //                submit(req, tag);
      //              } catch (AlreadyClosedException e) {
      //
      //              }
      //            } else {
      //              if (coreDesc != null) {
      //                try {
      //                  Boolean indexChanged = req.cmd.isIndexChanged.get();
      //                  if (zkShardTerms != null && (indexChanged == null || indexChanged)) {
      //                    zkShardTerms.ensureHighestTermsAreNotZero();
      //                    zkShardTerms.ensureTermsIsHigher(coreDesc.getName(), Collections.singleton(req.node.getNodeProps().getName()));
      //                  }
      //
      //                } catch (Exception e) {
      //                  log.error("Exception in dist update", e);
      //                }
      //              }
      //            }
      //          }
      //        });
      //
      //      }
    } catch (Exception e) {
      log.error("Exception sending dist update", e);
      try {

        if (e instanceof Http2SolrClient.CancelledException) {
          log.info("Stream cancelled msg={}", e.getMessage());
          return;
        }

        Error error = new Error(tag);
        error.t = e;
        error.req = req;
        if (e instanceof SolrException) {
          error.statusCode = ((SolrException) e).code();
        }
        if (checkRetry(error)) {
          log.info("Retrying distrib update on error: {}", e.getMessage());
          submit(req, "root");
        } else {
          allErrors.put(req, error);
        }
      } finally {
        asyncTracker.arrive();
      }
    }
  }

  public static class Req {
    public final Node node;
    public final UpdateRequest uReq;
    public final AtomicInteger retries = new AtomicInteger();
    public final boolean synchronous;
    public final UpdateCommand cmd;
    final private RollupRequestReplicationTracker rollupTracker;
    final private LeaderRequestReplicationTracker leaderTracker;
    public final boolean requiresWait;
    public boolean isCommit;

    public Req(UpdateCommand cmd, Node node, UpdateRequest uReq, boolean synchronous, boolean requiresWait) {
      this(cmd, node, uReq, synchronous, requiresWait, null, null);
    }

    public Req(UpdateCommand cmd, Node node, UpdateRequest uReq, boolean synchronous, boolean requiresWait,
               RollupRequestReplicationTracker rollupTracker,
               LeaderRequestReplicationTracker leaderTracker) {
      this.node = node;
      this.uReq = uReq;
      this.synchronous = synchronous;
      this.requiresWait = requiresWait;
      this.cmd = cmd;
      this.rollupTracker = rollupTracker;
      this.leaderTracker = leaderTracker;
    }

    public String toString() {
      return "SolrCmdDistributor$Req: cmd=" + cmd.toString() + "; node=" + node;
    }

    // Called whenever we get results back from a sub-request.
    // The only ambiguity is if I have _both_ a rollup tracker and a leader tracker. In that case we need to handle
    // both requests returning from leaders of other shards _and_ from my followers. This happens if a leader happens
    // to be the aggregator too.
    //
    // This isn't really a problem because only responses _from_ some leader will have the "rf" parameter, in which case
    // we need to add the data to the rollup tracker.
    //
    // In the case of a leaderTracker and rollupTracker both being present, then we need to take care when assembling
    // the final response to check both the rollup and leader trackers on the aggregator node.

    // Variant used by the async (Http2) path, which hands us an already-parsed response NamedList rather than the
    // raw response body. Gated so it is essentially free (two null checks) when the client did not request
    // replication-factor tracking (the common case: no leader/rollup tracker allocated).
    public void trackRequestResult(NamedList<?> result, boolean success) {
      if (leaderTracker == null && rollupTracker == null) {
        return;
      }

      // Integer.MAX_VALUE means there was no "rf" on the response (e.g. a plain replica forward), in which
      // case the leader just increments its own achieved rf for this successful forward.
      int rfFromResp = Integer.MAX_VALUE;
      if (result != null) {
        Object hdr = result.get("responseHeader");
        if (hdr instanceof NamedList) {
          Object rfObj = ((NamedList<?>) hdr).get(UpdateRequest.REPFACT);
          if (rfObj instanceof Integer) {
            rfFromResp = (Integer) rfObj;
          }
        }
      }

      if (leaderTracker != null && rfFromResp == Integer.MAX_VALUE) {
        leaderTracker.trackRequestResult(node, success);
      }
      if (rollupTracker != null) {
        rollupTracker.testAndSetAchievedRf(rfFromResp);
      }
    }

    public void trackRequestResult(org.eclipse.jetty.client.api.Response resp, InputStream respBody, boolean success) {

      // Returning Integer.MAX_VALUE here means there was no "rf" on the response, therefore we just need to increment
      // our achieved rf if we are a leader, i.e. have a leaderTracker.
      int rfFromResp = getRfFromResponse(respBody);

      if (leaderTracker != null && rfFromResp == Integer.MAX_VALUE) {
        leaderTracker.trackRequestResult(node, success);
      }

      if (rollupTracker != null) {
        rollupTracker.testAndSetAchievedRf(rfFromResp);
      }
    }

    private int getRfFromResponse(InputStream inputStream) {
      if (inputStream != null) {
        try {
          BinaryResponseParser brp = new BinaryResponseParser();
          NamedList<Object> nl = brp.processResponse(inputStream, null);
          Object hdr = nl.get("responseHeader");
          if (hdr instanceof NamedList) {
            @SuppressWarnings({"unchecked"})
            NamedList<Object> hdrList = (NamedList<Object>) hdr;
            Object rfObj = hdrList.get(UpdateRequest.REPFACT);
            if (rfObj instanceof Integer) {
              return (Integer) rfObj;
            }
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          log.warn("Failed to parse response replication factor accounting node={}", node, e);
        }
      }
      return Integer.MAX_VALUE;
    }
  }

  public static volatile Diagnostics.Callable testing_errorHook;  // called on error when forwarding request.  Currently data=[this, Request]

  public static class Error {
    public final String tag;
    public volatile Throwable t;
    public volatile int statusCode = -1;

    public volatile Req req;

    public Error(String tag) {
      this.tag = tag;
    }
    
    public String toString() {
      return "SolrCmdDistributor$Error: statusCode=" + statusCode + "; throwable=" + t + "; req=" + req + "; tag=" + tag;
    }
  }
  
  public static abstract class Node {
    public abstract String getUrl();
    public abstract boolean checkRetry();
    public abstract String getCoreName();
    public abstract String getBaseUrl();
    public abstract Replica getNodeProps();
    public abstract String getCollection();
    public abstract String getShardId();
    public abstract int getMaxRetries();
  }

  public static class StdNode extends Node {
    protected final ZkStateReader zkStateReader;
    protected final AtomicReference<Replica> nodeProps = new AtomicReference<>();
    protected final String collection;
    protected final String shardId;
    private final boolean retry;
    private final int maxRetries;

    public StdNode(ZkStateReader zkStateReader, Replica nodeProps) {
      this(zkStateReader, nodeProps, null, null, 0);
    }
    
    public StdNode(ZkStateReader zkStateReader, Replica nodeProps, String collection, String shardId) {
      this(zkStateReader, nodeProps, collection, shardId, 0);
    }
    
    public StdNode(ZkStateReader zkStateReader, Replica nodeProps, String collection, String shardId, int maxRetries) {
      this.zkStateReader = zkStateReader;
      this.nodeProps.set(nodeProps);
      this.collection = collection;
      this.shardId = shardId;
      this.retry = maxRetries > 0;
      this.maxRetries = maxRetries;
      if (nodeProps == null) {
        SolrException e = new SolrException(SolrException.ErrorCode.SERVER_ERROR, "nodeProps cannot be null");
        log.error("nodeProps cannot be null", e);
        throw e;
      }
    }
    
    public String getCollection() {
      return collection;
    }
    
    public String getShardId() {
      return shardId;
    }
        
    @Override
    public String getUrl() {
      return nodeProps.get().getCoreUrl();
    }
    
    @Override
    public String toString() {
      return this.getClass().getSimpleName() + ": " + nodeProps.get().getCoreUrl();
    }

    @Override
    public boolean checkRetry() {
      return false;
    }

    @Override
    public String getBaseUrl() {
      return nodeProps.get().getBaseUrl();
    }

    @Override
    public String getCoreName() {
      return nodeProps.get().getName();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      String baseUrl = nodeProps.get().getBaseUrl();
      String coreName = nodeProps.get().getName();
      String url = nodeProps.get().getCoreUrl();
      result = prime * result + ((baseUrl == null) ? 0 : baseUrl.hashCode());
      result = prime * result + ((coreName == null) ? 0 : coreName.hashCode());
      result = prime * result + ((url == null) ? 0 : url.hashCode());
      result = prime * result + Boolean.hashCode(retry);
      result = prime * result + Integer.hashCode(maxRetries);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      StdNode other = (StdNode) obj;
      if (this.retry != other.retry) return false;
      if (this.maxRetries != other.maxRetries) return false;
      Replica replica = nodeProps.get();
      String baseUrl = replica.getBaseUrl();
      String coreName = replica.getName();
      String url = replica.getCoreUrl();
      Replica otherReplica = other.nodeProps.get();
      if (baseUrl == null) {
        if (otherReplica.getBaseUrl() != null) return false;
      } else if (!baseUrl.equals(otherReplica.getBaseUrl())) return false;
      if (coreName == null) {
        if (otherReplica.getName() != null) return false;
      } else if (!coreName.equals(otherReplica.getName())) return false;
      if (url == null) {
        return otherReplica.getCoreUrl() == null;
      } else return url.equals(otherReplica.getCoreUrl());
    }

    @Override
    public Replica getNodeProps() {
      return nodeProps.get();
    }

    @Override
    public int getMaxRetries() {
      return this.maxRetries;
    }
  }
  
  // RetryNodes are used in the case of 'forward to leader' where we want
  // to try the latest leader on a fail in the case the leader just went down.
  public static class ForwardNode extends StdNode {
    
    public ForwardNode(ZkStateReader zkStateReader, Replica nodeProps, String collection, String shardId) {
      super(zkStateReader, nodeProps, collection, shardId);
    }

    @Override
    public boolean checkRetry() {
      log.debug("check retry");
      Replica leaderProps;
      try {
        leaderProps = zkStateReader.getLeaderRetry(
            collection, shardId);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        return false;
      } catch (Exception e) {
        // we retry with same info
        log.warn(null, e);
        return true;
      }

      this.nodeProps.set(leaderProps);

      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
          + ((collection == null) ? 0 : collection.hashCode());
      result = prime * result + ((shardId == null) ? 0 : shardId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      ForwardNode other = (ForwardNode) obj;
      Replica replica = nodeProps.get();
      Replica otherReplica = other.nodeProps.get();
      if (replica.getCoreUrl() == null) {
        return otherReplica.getCoreUrl() == null;
      } else return replica.getCoreUrl().equals(otherReplica.getCoreUrl());
    }
  }

  public Map<Object, Error> getErrors() {
    return allErrors;
  }

  private static class AlreadyClosedUpdateCmd extends UpdateCommand {
    public AlreadyClosedUpdateCmd(SolrQueryRequest req) {
      super(req);
    }

    @Override
    public String name() {
      return "AlreadyClosedException";
    }
  }
}


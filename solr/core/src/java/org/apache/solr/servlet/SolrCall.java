package org.apache.solr.servlet;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.QoSParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.*;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.QueryResponseWriterUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.servlet.AsyncContext;
import javax.servlet.ServletInputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.REMOTEQUERY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETRY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETURN;

public abstract class SolrCall {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static final Random random;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      random = new Random();
    } else {
      random = new Random(seed.hashCode());
    }
  }

  private final AtomicReference<List<CommandOperation>> parsedCommands = new AtomicReference<>();

  public static final String ORIGINAL_USER_PRINCIPAL_HEADER = "originalUserPrincipal";

  public String getCt() {
    return ct;
  }

  String ct;

  public abstract SolrDispatchFilter.Action call() throws IOException;

  protected static String extractRemotePath(CoreContainer cores, String collectionName, SolrParams queryParams, HttpServletRequest request) throws SolrException {
    String source = request.getHeader(QoSParams.REQUEST_SOURCE);
    boolean externalRequest = (source == null || !source.equals(QoSParams.INTERNAL));
    if (!externalRequest) {
      // Internal requests are normally already routed to a node hosting the target core, so we
      // avoid proxying them to prevent loops. However, a collection-addressed internal request
      // (e.g. an internal /stream request that SolrStream sends to an arbitrary node) can still
      // land on a node with no local replica of that collection. Allow a single proxy hop for
      // those; the X-Forwarded-For header that remoteQuery() sets on proxied requests guards
      // against proxy loops (an already-proxied internal request returns null here).
      if (request.getHeader(HttpHeader.X_FORWARDED_FOR.toString()) != null) {
        return null;
      }
      // Never proxy an internal MULTI-collection request (e.g. /solr/collection1,collection2/select):
      // those are distributed-search coordinator requests that any node hosting ANY of the listed
      // collections handles by fanning out to remote shards. Proxying the whole request to a single
      // collection's core would re-run the distributed search there and double-count results.
      String uri = request.getRequestURI();
      if (uri != null && (uri.indexOf(',') >= 0 || uri.toLowerCase(Locale.ENGLISH).indexOf("%2c") >= 0)) {
        return null;
      }
    }

    String coreUrl = getRemoteCoreUrl(cores, collectionName);

    if (coreUrl != null
        && queryParams.get(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM) == null) {
      //      if (invalidStates != null) {
      //        //it does not make sense to send the request to a remote node
      //        throw new SolrException(SolrException.ErrorCode.INVALID_STATE, new String(Utils.toJSON(invalidStates), org.apache.lucene.util.IOUtils.UTF_8));
      //      }
      return coreUrl;
    }
    return coreUrl;
  }

  protected static void sendError(Throwable ex, HttpServletResponse response) throws IOException {
    log.error("ERROR", ex);
    SimpleOrderedMap info = new SimpleOrderedMap();
    int code = ResponseUtils.getErrorInfo(ex, info, log);
    sendError(code, info.toString(), response);
  }

  protected static void sendError(int code, String message, HttpServletResponse response) throws IOException {
    log.error("sendError={}", message);
    try {
      response.sendError(code, message);
    } catch (EOFException e) {
      log.info("Unable to write error response, client closed connection or we are shutting down", e);
    }
  }

  private static String getCoreUrl(CoreContainer cores, Collection<Slice> slices) {
    String coreUrl;
    List<Replica> randomizedReplicas = new ArrayList<>();
    for (Slice slice : slices) {
      randomizedReplicas.clear();
      randomizedReplicas.addAll(slice.getReplicas());
      Collections.shuffle(randomizedReplicas, random);

      for (Replica replica : randomizedReplicas) {
        log.debug("check replica {} with node name {} against live nodes {} with state {}", replica.getName(), replica.getNodeName(),
            cores.getZkController().getZkStateReader().getLiveNodes(), replica.getState());
        if (!replica.getNodeName().equals(cores.getZkController().getNodeName()) && cores.getZkController().zkStateReader.getLiveNodes()
            .contains(replica.getNodeName()) && replica.getState() == Replica.State.ACTIVE) {

          coreUrl = replica.getCoreUrl();

          return coreUrl;
        }
      }
    }

    return null;
  }

  protected static SolrCore getCoreByCollection(CoreContainer cores, String collectionName, boolean isPreferLeader, HttpServletRequest request) {

    // Note: unlike extractRemotePath(), we do NOT bail out for internal requests here. Resolving a
    // collection-addressed request to a *local* core can never cause a proxy loop, and internal
    // clients (e.g. the streaming SolrClientCache's CloudHttp2SolrClient, which marks every request
    // internal) routinely address requests to a collection name rather than a specific core. Bailing
    // here would leave such requests unresolved and they would fall through to a 404.

    log.debug("get core by collection {} {}", collectionName, isPreferLeader);

    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();

    DocCollection collection = zkStateReader.getCollectionOrNull(collectionName);

    if (collection == null) {
      log.debug("no local core found for collection={}", collectionName);
      return null;
    }

    if (isPreferLeader) {
      List<Replica> leaderReplicas = collection.getLeaderReplicas(cores.getZkController().getNodeName(), zkStateReader.getLiveNodes());
      log.debug("preferLeader leaderReplicas={}", leaderReplicas);
      SolrCore core = randomlyGetSolrCore(collection, cores, leaderReplicas, true);
      if (core != null) return core;
    }

    List<Replica> replicas = collection.getReplicas(cores.getZkController().getNodeName());
    if (replicas.size() == 0) {
      log.info("replicas={} node={} docReplicas={}", replicas, cores.getZkController().getNodeName(), collection.getReplicas());
    }
    if (log.isDebugEnabled()) log.debug("replicas for node {} {}", replicas, cores.getZkController().getNodeName());
    SolrCore returnCore = randomlyGetSolrCore(collection, cores, replicas, true);
    if (log.isDebugEnabled()) log.debug("returning core by collection {}", returnCore == null ? null : returnCore.getName());
    return returnCore;
  }

  private static SolrCore randomlyGetSolrCore(DocCollection collection, CoreContainer cores, List<Replica> replicas, boolean checkActive) {
    log.info("randomlyGetSolrCore docCollection={} {}", collection, replicas);
    if (replicas != null) {
      final Set<String> liveNodes = cores.getZkController().getZkStateReader().getLiveNodes();

      // First pass: prefer a locally-registered core whose replica is ACTIVE and live. A DOWN /
      // RECOVERING local replica must not serve queries when an active replica is available to
      // forward to (returning null here lets the caller proxy to an active remote replica).
      HttpSolrCall.RandomIterator<Replica> it = new HttpSolrCall.RandomIterator<>(random, replicas);
      while (it.hasNext()) {
        Replica replica = it.next();
        if (!checkActive
            || (replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName()))) {
          SolrCore core = checkProps(cores, replica);
          if (core != null) {
            return core;
          }
        }
      }

      if (!checkActive) {
        return null;
      }

      // Second pass (anti-404 fallback): only if the collection has NO active+live replica anywhere
      // to forward to, serve from a locally-registered core regardless of its (possibly lagging) ZK
      // state. ZK state can briefly lag local core registration; in that window, with nowhere else to
      // route, serving locally beats a spurious 404.
      boolean hasActiveAnywhere = false;
      for (Replica r : collection.getReplicas()) {
        if (r.getState() == Replica.State.ACTIVE && liveNodes.contains(r.getNodeName())) {
          hasActiveAnywhere = true;
          break;
        }
      }
      if (!hasActiveAnywhere) {
        HttpSolrCall.RandomIterator<Replica> it2 = new HttpSolrCall.RandomIterator<>(random, replicas);
        while (it2.hasNext()) {
          Replica replica = it2.next();
          SolrCore core = checkProps(cores, replica);
          if (core != null) {
            return core;
          }
        }
      }
    }

    return null;
  }

  /**
   * Resolves the parameter as a potential comma delimited list of collections, and resolves aliases too.
   * One level of aliases pointing to another alias is supported.
   * De-duplicates and retains the order.
   * {@link #getCollectionsList()}
   */
  protected static List<String> resolveCollectionListOrAlias(CoreContainer cores, String collectionStr) {
    if (collectionStr == null || StringUtils.isBlank(collectionStr)) {
      return Collections.emptyList();
    }
    List<String> result = Collections.emptyList();
    LinkedHashSet<String> uniqueList = null;
    Aliases aliases = getAliases(cores);
    List<String> inputCollections = StrUtils.splitSmart(collectionStr, ",", true);
    if (inputCollections.size() > 1) {
      uniqueList = new LinkedHashSet<>();
    }
    for (String inputCollection : inputCollections) {
      List<String> resolvedCollections = aliases.resolveAliases(inputCollection);
      if (uniqueList != null) {
        uniqueList.addAll(resolvedCollections);
      } else {
        result = resolvedCollections;
      }
    }
    if (uniqueList != null) {
      return List.copyOf(uniqueList);
    } else {
      return Collections.unmodifiableList(result);
    }
  }

  protected static Aliases getAliases(CoreContainer cores) {
    return cores.isZooKeeperAware() ? cores.getZkController().getZkStateReader().getAliases() : Aliases.EMPTY;
  }

  protected static String getRemoteCoreUrl(CoreContainer cores, String collectionName) throws SolrException {
    log.info("getRemoteCoreUrl for {}", collectionName);

    final ZkStateReader reader = cores.getZkController().getZkStateReader();

    // Fast path: if the requested name is not a known collection it is almost certainly a CORE name
    // addressed directly (e.g. a leader->replica update forward, or /solr/<coreName>/... for a core on
    // another node). In that case resolve it straight to the hosting node. This avoids the 3s
    // waitForState below - which can never succeed for a core name and would otherwise stall every such
    // request for the full timeout (and spam NoNode/Timeout errors for /collections/<coreName>/state.json).
    // Under heavy concurrent indexing these stalls pile up and exhaust the request thread pool, which can
    // in turn make a healthy replica look unresponsive and get dropped from cluster state.
    if (reader.getCollectionOrNull(collectionName) == null) {
      String byCore = resolveRemoteCore(cores, collectionName);
      if (byCore != null) {
        if (log.isDebugEnabled()) {
          log.debug("get remote core url resolved by core name {} for {}", byCore, collectionName);
        }
        // Known core name: either an active remote URL, or empty meaning the core is known but not
        // currently servable remotely (down/recovering/being deleted). Either way do NOT waitForState
        // on a collection that can never exist by this name.
        return byCore.isEmpty() ? null : byCore;
      }
      // Not a known core either - it may be a collection whose state has not propagated to this node
      // yet, so fall through to the wait below.
    }

    try {
      reader.waitForState(collectionName, 3, TimeUnit.SECONDS, (liveNodes1, coll) -> {
        if (coll == null) {
          return false;
        }
        for (Slice slice : coll.getSlices()) {

          for (Replica replica : slice.getReplicas()) {
            log.debug("check replica {} with node name {} against live nodes {} with state {}", replica.getName(), replica.getNodeName(),
                cores.getZkController().getZkStateReader().getLiveNodes(), replica.getState());
            if (!replica.getNodeName().equals(cores.getZkController().getNodeName()) && cores.getZkController().zkStateReader.getLiveNodes()
                .contains(replica.getNodeName()) && replica.getState() == Replica.State.ACTIVE) {

              return true;
            }
          }
        }
        return false;
      });
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
    }

    final DocCollection docCollection = cores.getZkController().getZkStateReader().getCollectionOrNull(collectionName);
    if (docCollection == null || docCollection.getSlices()== null) {
      // collectionName is not a collection: it may be a CORE name addressed directly
      // (e.g. /solr/<coreName>/... for a core that lives on another node). Upstream resolves
      // these by scanning every collection for a replica whose core name matches and proxying
      // there. In this fork the replica name IS the core name, so match on replica.getName().
      return getCoreUrlByCoreName(cores, collectionName);
    }

    String coreUrl = getCoreUrl(cores, docCollection.getSlices());
    if (coreUrl == null) {
      // No remote replica found for the collection; as a last resort treat the name as a core name.
      coreUrl = getCoreUrlByCoreName(cores, collectionName);
    }

    if (log.isDebugEnabled()) {
      log.debug("get remote core url returning {} for {}", coreUrl, collectionName);
      }
    return coreUrl;
  }

  /**
   * Resolve a request that addresses a core by name (rather than by collection) to the base URL of a
   * remote node hosting that core, so the request can be proxied there. This restores upstream's
   * by-core-name remote forwarding (e.g. /solr/&lt;coreName&gt;/select for a core on another node).
   * Scans all collections; in this fork the replica name IS the core name, so we match on
   * replica.getName() rather than the (always-null) CORE_NAME_PROP. Rare fallback path only.
   */
  private static String getCoreUrlByCoreName(CoreContainer cores, String coreName) {
    String r = resolveRemoteCore(cores, coreName);
    return (r == null || r.isEmpty()) ? null : r;
  }

  /**
   * Single-scan resolver for a core (replica) name. Returns the active remote core URL if a remote,
   * live, ACTIVE replica with that name exists; an empty string if the name IS a known replica name but
   * is not currently servable from a remote node (local-only, down, recovering, or being deleted); or
   * {@code null} if the name does not correspond to any known replica at all (so it may instead be a
   * collection whose state has not yet propagated). Distinguishing "known core" from "unknown" lets the
   * caller avoid a pointless multi-second waitForState on a collection that can never exist.
   */
  static String resolveRemoteCore(CoreContainer cores, String coreName) {
    if (coreName == null) {
      return null;
    }
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    String localNodeName = cores.getZkController().getNodeName();
    Set<String> liveNodes = zkStateReader.getLiveNodes();
    Map<String,DocCollection> collections = zkStateReader.getClusterState().getCollectionsMap();
    boolean known = false;
    for (DocCollection collection : collections.values()) {
      if (collection == null) {
        continue;
      }
      for (Slice slice : collection.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          if (coreName.equals(replica.getName())) {
            known = true;
            if (!replica.getNodeName().equals(localNodeName)
                && liveNodes.contains(replica.getNodeName())
                && replica.getState() == Replica.State.ACTIVE) {
              return replica.getCoreUrl();
            }
          }
        }
      }
    }
    return known ? "" : null;
  }

  public abstract SolrParams getSolrParams();

  protected void writeResponse(SolrQueryRequest solrReq, SolrQueryResponse solrRsp, HttpServletRequest req, HttpServletResponse response,
      QueryResponseWriter responseWriter, Method reqMethod)
      throws IOException {
    try {


      Map invalidStates = (Map) solrReq.getContext().get(BaseCloudSolrClient.STATE_VERSION);
      //This is the last item added to the response and the client would expect it that way.
      //If that assumption is changed , it would fail. This is done to avoid an O(n) scan on
      // the response for each request
      if (invalidStates != null && invalidStates.size() > 0) {
        solrRsp.add(BaseCloudSolrClient.STATE_VERSION, invalidStates);
      }
      // Now write it out

      // don't call setContentType on null
      if (null != ct) response.setContentType(ct);

      if (solrRsp.getException() != null) {
        NamedList info = new SimpleOrderedMap();
        int code = ResponseUtils.getErrorInfo(solrRsp.getException(), info, log);
        solrRsp.add("error", info);
        response.setStatus(code);
      }


      if (Method.HEAD != reqMethod) {
        ExpandableDirectBufferOutputStream outStream = QueryResponseWriterUtil.writeQueryResponse(responseWriter, solrReq, solrRsp, req, response, ct);

        ByteBuffer buffer = outStream.buffer().byteBuffer().asReadOnlyBuffer();
        buffer.position(outStream.offset() + outStream.buffer().wrapAdjustment());
        buffer.limit(outStream.position() + outStream.buffer().wrapAdjustment());

        response.setContentLength((int) outStream.size());
        // DO NOT re-enable this WriteListener async-write path until the destroy()/writeResponse
        // lifecycle is fixed. It is currently latent because it requires BOTH solr.asyncIO=true
        // (ASYNC_IO) and solr.asyncDispatchFilter=true (ASYNC, which makes req.isAsyncStarted()
        // true) — both default false. When enabled, SolrDispatchFilter.filter()'s finally runs
        // call.destroy() (closing solrCore, releasing the pooled buffer) the instant call() returns,
        // while this WriteListener defers the buffer write/release to a later Jetty callback →
        // use-after-free of the pooled Agrona buffer. Synchronous sendContent below is the safe path.
        if (req.isAsyncStarted() && SolrDispatchFilter.ASYNC_IO) {

          HttpOutput out = (HttpOutput) response.getOutputStream();
          //out.setWriteListener(new MyWriteListener(out, buffer, req, response));
          out.setWriteListener(new WriteListener() {
            @Override
            public void onWritePossible() throws IOException {

              try {
                //SolrDispatchFilter.consumeInputFully((ServletInputStream) solrReq.getContentStreams().iterator().next().getStream());
                while (true) {
                  boolean ready = out.isReady();
                  if (!ready ||  out.isClosed()) break;
                  if (!buffer.hasRemaining()) {
                    req.getAsyncContext().complete();
                    ExpandableBuffers.getInstance().release(outStream.buffer());
                    return;
                  }
                  out.write(buffer);
                }

                req.getAsyncContext().complete();
              } catch (Exception e) {
                log.error("Solr ran into an unexpected problem", e);
                try {
                  //  response.sendError(code, t.getClass().getName() + ' ' + t.getMessage());
                  SolrDispatchFilter.sendException(e, SolrCall.this, req, response);
                } finally {
                  req.getAsyncContext().complete();
                }
              }

            }

            @Override
            public void onError(Throwable t) {
              log.error("Solr ran into an unexpected problem", t);
              try {

                //  response.sendError(code, t.getClass().getName() + ' ' + t.getMessage());
                SolrDispatchFilter.sendException(t, SolrCall.this, req, response);
              } catch (IOException ioException) {
                log.warn("Exception sending error", t); // MRM TODO
              } finally {
                req.getAsyncContext().complete();
              }
            }
          });
          //          out.sendContent(buffer, new Callback() {
          //            @Override public void succeeded() {
          //              Callback.super.succeeded();
          //              try {
          //                ExpandableBuffers.getInstance().release(outStream.buffer());
          //              } finally {
          //                req.getAsyncContext().complete();
          //              //  out.softClose();
          //              }
          //            }
          //
          //            @Override public void failed(Throwable t) {
          //              Callback.super.failed(t);
          //
          //              log.error("Solr ran into an unexpected problem", t);
          //              try {
          //                ExpandableBuffers.getInstance().release(outStream.buffer());
          //              //  response.sendError(code, t.getClass().getName() + ' ' + t.getMessage());
          //                SolrDispatchFilter.sendException(t, SolrCall.this, req, response);
          //              } catch (IOException ioException) {
          //                log.warn("Exception sending error", t); // MRM TODO
          //              } finally {
          //                req.getAsyncContext().complete();
          //                out.softClose();
          //              }
          //            }
          //          });
        } else {
          HttpOutput out = (HttpOutput) response.getOutputStream();
          try {
            out.sendContent(buffer);
          } finally {
            ExpandableBuffers.getInstance().release(outStream.buffer());
          }
        }
      }
      //else http HEAD request, nothing to write out, waited this long just to get ContentType
    } catch (EOFException e) {
      log.info("Unable to write response, client closed connection or we are shutting down", e);
    }
  }

  /**
   * Sets the "collection" parameter on the request to the list of alias-resolved collections for this request.
   * It can be avoided sometimes.
   * Note: {@link org.apache.solr.handler.component.HttpShardHandler} processes this param.
   * @see #getCollectionsList()
   */
  protected static void addCollectionParamIfNeeded(CoreContainer cores, SolrQueryRequest solrReq, SolrParams queryParams, List<String> collections) {
    if (collections.isEmpty()) {
      return;
    }
    assert cores.isZooKeeperAware();
    String collectionParam = queryParams.get(COLLECTION_PROP);
    // if there is no existing collection param and the core we go to is for the expected collection,
    //   then we needn't add a collection param
    SolrCore core = solrReq.getCore();
    if (collectionParam == null && // if collection param already exists, we may need to over-write it
        core != null && collections.equals(Collections.singletonList(core.getCoreDescriptor().getCollectionName()))) {
      return;
    }
    String newCollectionParam = StrUtils.join(collections, ',');
    if (newCollectionParam.equals(collectionParam)) {
      return;
    }
    // TODO add a SolrRequest.getModifiableParams ?
    ModifiableSolrParams params = new ModifiableSolrParams(solrReq.getParams());
    params.set(COLLECTION_PROP, newCollectionParam);
    solrReq.setParams(params);
  }

  /** The collection(s) referenced in this request. */
  public abstract List<String> getCollectionsList();

  public abstract AuthorizationContext.RequestType getRequestType();

 
  protected SolrDispatchFilter.Action authorize(CoreContainer cores, SolrQueryRequest solrReq, HttpServletRequest req, HttpServletResponse response) throws IOException {
    AuthorizationContext context = getAuthCtx(solrReq, req, getPath(), getRequestType());

    log.debug("AuthorizationContext : {}", context);
    AuthorizationResponse authResponse = cores.getAuthorizationPlugin().authorize(context);
    int statusCode = authResponse.statusCode;
    log.info("Authorization response status code {}", authResponse.statusCode);

    if (statusCode == AuthorizationResponse.PROMPT.statusCode) {
      Map<String, String> headers = (Map) req.getAttribute(AuthenticationPlugin.class.getName());
      if (headers != null) {
        headers.forEach(response::setHeader);
      }
      if (log.isDebugEnabled()) {
        log.debug("USER_REQUIRED {} {}", req.getHeader("Authorization"), req.getUserPrincipal());
      }
      // Write a STRUCTURED error (javabin/the request wt) via sendException, not response.sendError
      // (which renders a Jetty text/html page). A javabin/octet-stream client rejects the HTML body
      // with "Expected mime type application/octet-stream but got text/html" and loses both the real
      // status code and the message. Same fix as the V2HttpCall error-serialization path.
      SolrDispatchFilter.sendException(
          new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
              "Unauthorized request, authentication required. Response code: " + statusCode), this, req, response);
      if (shouldAudit(cores ,AuditEvent.EventType.REJECTED)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.REJECTED, req, context));
      }
      return RETURN;
    }
    if (statusCode == AuthorizationResponse.FORBIDDEN.statusCode) {
      if (log.isDebugEnabled()) {
        log.debug("UNAUTHORIZED auth header {} context : {}, msg: {}", req.getHeader("Authorization"), context, authResponse.getMessage());
      }
      SolrDispatchFilter.sendException(
          new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
              "Unauthorized request, Response code: " + statusCode), this, req, response);
      if (shouldAudit(cores, AuditEvent.EventType.UNAUTHORIZED)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.UNAUTHORIZED, req, context));
      }
      return RETURN;
    }
    if (!(statusCode == HttpStatus.SC_ACCEPTED) && !(statusCode == HttpStatus.SC_OK)) {
      log.warn("ERROR {} during authentication: {}", statusCode, authResponse.getMessage());
      SolrDispatchFilter.sendException(
          new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
              "ERROR during authorization, Response code: " + statusCode), this, req, response);
      if (shouldAudit(cores, AuditEvent.EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.ERROR, req, context));
      }
      return RETURN;
    }
    if (shouldAudit(cores, AuditEvent.EventType.AUTHORIZED)) {
      cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.AUTHORIZED, req, context));
    }

    return ADMIN;
  }

  public abstract String getPath();

  public abstract void destroy();

  protected static boolean shouldAudit(CoreContainer cores) {
    return cores.getAuditLoggerPlugin() != null;
  }

  protected static boolean shouldAudit(CoreContainer cores, AuditEvent.EventType eventType) {
    return shouldAudit(cores) && cores.getAuditLoggerPlugin().shouldLog(eventType);
  }

  protected static boolean shouldAuthorize(CoreContainer cores, String path, HttpServletRequest req) {
    if(PublicKeyHandler.PATH.equals(path)) return false;
    //admin/info/key is the path where public key is exposed . it is always unsecured
    if ("/".equals(path) || "/solr/".equals(path)) return false; // Static Admin UI files must always be served
    if (cores.getPkiAuthenticationPlugin() != null && req.getUserPrincipal() != null) {
      boolean b = PKIAuthenticationPlugin.needsAuthorization(req);
      log.debug("PkiAuthenticationPlugin says authorization required : {} ", b);
      return b;
    }
    return true;
  }

  protected void handleAdminRequest(CoreContainer cores, HttpServletRequest req, HttpServletResponse response) throws IOException {
    SolrQueryResponse solrResp = new SolrQueryResponse();
    SolrQueryRequest solrReq = getSolrReq();
    SolrCore.preDecorateResponse(solrReq, solrResp);
    handleAdmin(solrResp);
    SolrCore.postDecorateResponse(_getHandler(), solrReq, solrResp);
    //    if (log.isInfoEnabled() && solrResp.getToLog().size() > 0 && !path.startsWith("/admin/metrics")) {
    //      log.info(solrResp.getToLogAsString("[admin]"));
    //    }
    QueryResponseWriter respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get(solrReq.getParams().get(CommonParams.WT));
    if (respWriter == null) respWriter = getResponseWriter(solrReq);
    writeResponse(solrReq, solrResp, req, response, respWriter, Method.getMethod(req.getMethod()));
    if (shouldAudit(cores)) {
      AuditEvent.EventType eventType = solrResp.getException() == null ? AuditEvent.EventType.COMPLETED : AuditEvent.EventType.ERROR;
      if (shouldAudit(cores, eventType)) {
        cores.getAuditLoggerPlugin().doAudit(
            new AuditEvent(eventType, req, getAuthCtx(solrReq, req, getPath(), getRequestType()), solrReq.getRequestTimer().getTime(), solrResp.getException()));
      }
    }
  }

  protected abstract void handleAdmin(SolrQueryResponse solrResp);

  private static SolrCore checkProps(CoreContainer cores, Replica replica) {
    SolrCore core = null;
    boolean nodeMatches = cores.getZkController().getNodeName().equals(replica.getNodeName());
    if (nodeMatches) {
      core = cores.getCore(replica.getName());
      if (core == null) {
        // Bound the wait for a still-loading core (mirrors HttpSolrCall's 30s cap): an unbounded
        // poll parks the request thread forever on a wedged load and saturates the request pool.
        // On timeout fall through with whatever getCore returns (possibly null) rather than blocking.
        final long deadline = System.currentTimeMillis() + 30_000L;
        while (cores.isCoreLoading(replica.getName())) {
          if (System.currentTimeMillis() > deadline) {
            log.warn("Timed out waiting for core {} to finish loading", replica.getName());
            break;
          }
          try {
            Thread.sleep(150);
          } catch (InterruptedException interruptedException) {
            ParWork.propagateInterrupt(interruptedException);
            break;
          }
        }
        core = cores.getCore(replica.getName());
      }
    }
    log.debug("check local core has correct props replica={} nodename={} nodematches={} core found={}", replica, cores.getZkController().getNodeName(), nodeMatches, core != null);
    return core;
  }

  public abstract SolrQueryRequest getSolrReq();

  /**
   * Returns {@link QueryResponseWriter} to be used.
   * When {@link CommonParams#WT} not specified in the request or specified value doesn't have
   * corresponding {@link QueryResponseWriter} then, returns the default query response writer
   * Note: This method must not return null
   */
  public static QueryResponseWriter getResponseWriter(SolrQueryRequest solrReq) {
    String wt = solrReq.getParams().get(CommonParams.WT);
    SolrCore core = solrReq.getCore();
    if (core != null) {
      return core.getQueryResponseWriter(wt);
    } else {
      return SolrCore.DEFAULT_RESPONSE_WRITERS.getOrDefault(wt,
          SolrCore.DEFAULT_RESPONSE_WRITERS.get("standard"));
    }
  }

  public List<CommandOperation> getCommands(SolrQueryRequest solrReq, boolean validateInput) {

    return parsedCommands.updateAndGet(commandOperations -> {
      if (commandOperations == null) {
        Iterable<ContentStream> contentStreams = null;

        contentStreams = solrReq.getContentStreams();

        if (contentStreams == null) return Collections.EMPTY_LIST;
        else {
          return Collections.unmodifiableList(ApiBag.getCommandOperations(contentStreams.iterator().next(), getValidators(), validateInput));
        }
      }
      return commandOperations;
    });
  }

  protected abstract Map<Object,JsonSchemaValidator> getValidators();

  protected AuthorizationContext getAuthCtx(SolrQueryRequest solrReq, HttpServletRequest req, String path, AuthorizationContext.RequestType requestType) {

    String resource = getPath();

    int idx = resource.indexOf('/');
    int idx2 = -1;

    if (idx > -1) {

      idx2 = resource.indexOf('/', 1);
      if (idx2 > 0) {
        // save the portion after the ':' for a 'handler' path parameter
        resource = resource.substring(idx + 1, idx2);

      } else {
        resource = resource.substring(idx + 1);
        log.debug("core parsed as {}", resource);
      }
    }

    SolrParams params = getSolrParams();
    final ArrayList<AuthorizationContext.CollectionRequest> collectionRequests = new ArrayList<>();
    for (String collection : getCollectionsList()) {
      collectionRequests.add(new AuthorizationContext.CollectionRequest(collection));
    }

    // Extract collection name from the params in case of a Collection Admin request
    if (resource.equals("/admin/collections")) {
      if (CREATE.isEqual(params.get("action"))||
          RELOAD.isEqual(params.get("action"))||
          DELETE.isEqual(params.get("action")))
        collectionRequests.add(new AuthorizationContext.CollectionRequest(params.get("name")));
      else if (params.get(COLLECTION_PROP) != null)
        collectionRequests.add(new AuthorizationContext.CollectionRequest(params.get(COLLECTION_PROP)));
    }

    // Populate the request type if the request is select or update
    // Use the full path (not the stripped collection-name prefix) for accurate classification.
    String fullPath = getPath();
    if(requestType == AuthorizationContext.RequestType.UNKNOWN) {
      if(fullPath.contains("/select") || fullPath.contains("/get"))
        requestType = AuthorizationContext.RequestType.READ;
      if(fullPath.contains("/update"))
        requestType = AuthorizationContext.RequestType.WRITE;
    }

    AuthorizationContext.RequestType finalRequestType = requestType;
    String finalResource = resource;
    return new AuthorizationContext() {
      @Override
      public SolrParams getParams() {
        return null == solrReq ? null : solrReq.getParams();
      }

      @Override
      public Principal getUserPrincipal() {
        return req.getUserPrincipal();
      }

      @Override
      public String getHttpHeader(String s) {
        return req.getHeader(s);
      }

      @Override
      public Enumeration<String> getHeaderNames() {
        return req.getHeaderNames();
      }

      @Override
      public List<CollectionRequest> getCollectionRequests() {
        return collectionRequests;
      }

      @Override
      public RequestType getRequestType() {
        return finalRequestType;
      }

      public String getResource() {
        return path;
      }

      @Override
      public String getHttpMethod() {
        return req.getMethod();
      }

      @Override
      public Object getHandler() {
        return _getHandler();
      }

      @Override
      public String toString() {
        StringBuilder response = new StringBuilder("userPrincipal: [").append(getUserPrincipal()).append("]")
            .append(" type: [").append(finalRequestType.toString()).append("], collections: [");
        for (CollectionRequest collectionRequest : collectionRequests) {
          response.append(collectionRequest.collectionName).append(", ");
        }
        if(collectionRequests.size() > 0)
          response.delete(response.length() - 1, response.length());

        response.append("], Path: [").append(finalResource).append("]");
        response.append(" path : ").append(getPath()).append(" params :").append(getParams());
        return response.toString();
      }

      @Override
      public String getRemoteAddr() {
        return req.getRemoteAddr();
      }

      @Override
      public String getRemoteHost() {
        return req.getRemoteHost();
      }
    };

  }

  protected abstract SolrRequestHandler _getHandler();

  protected SolrDispatchFilter.Action remoteQuery(HttpServletRequest req, HttpServletResponse response, String coreUrl, HttpClient httpClient, SolrParams parsedParams) throws IOException {
    SolrRequestForwarder.forward(req, response, coreUrl, httpClient, 120000L, parsedParams);
    return REMOTEQUERY;
  }

  protected static boolean hasContent(@Nonnull HttpServletRequest clientRequest) {
    return clientRequest.getContentLength() > 0 ||
        clientRequest.getContentType() != null ||
        clientRequest.getHeader(HttpHeader.TRANSFER_ENCODING.asString()) != null;
  }

  protected static void addProxyHeaders(@Nonnull HttpServletRequest clientRequest, Request proxyRequest) {
    proxyRequest.header(HttpHeader.VIA, "HTTP/2.0 Solr Proxy"); //MRM TODO: protocol hard code
    proxyRequest.header(HttpHeader.X_FORWARDED_FOR, clientRequest.getRemoteAddr());
    // we have some tricky to see in tests header size limitations
    // proxyRequest.header(HttpHeader.X_FORWARDED_PROTO, clientRequest.getScheme());
    // proxyRequest.header(HttpHeader.X_FORWARDED_HOST, clientRequest.getHeader(HttpHeader.HOST.asString()));
    // proxyRequest.header(HttpHeader.X_FORWARDED_SERVER, clientRequest.getLocalName());
    proxyRequest.header(QoSParams.REQUEST_SOURCE, QoSParams.INTERNAL);
  }

  protected static void copyRequestHeaders(@Nonnull HttpServletRequest clientRequest, Request proxyRequest) {
    // First clear possibly existing headers, as we are going to copy those from the client request.
    proxyRequest = proxyRequest.headers(httpFields -> httpFields.clear());

    Set<String> headersToRemove = findConnectionHeaders(clientRequest);

    for (Enumeration<String> headerNames = clientRequest.getHeaderNames(); headerNames.hasMoreElements();) {
      String headerName = headerNames.nextElement();
      String lowerHeaderName = headerName.toLowerCase(Locale.ENGLISH);

      boolean preserveHost = false;
      if (HttpHeader.HOST.is(headerName) && !preserveHost)
        continue;

      // Remove hop-by-hop headers.
      if (HOP_HEADERS.contains(lowerHeaderName))
        continue;
      if (headersToRemove != null && headersToRemove.contains(lowerHeaderName))
        continue;

      for (Enumeration<String> headerValues = clientRequest.getHeaders(headerName); headerValues.hasMoreElements();) {
        String headerValue = headerValues.nextElement();
        if (headerValue != null) {
          proxyRequest.headers(httpFields -> httpFields.add(headerName, headerValue));
          //System.out.println("request header: " + headerName + " : " + headerValue);
        }
      }
    }

    // Force the Host header if configured
    // if (_hostHeader != null)
    // proxyRequest.header(HttpHeader.HOST, _hostHeader);
  }

  /** Returns null if the state ({@link BaseCloudSolrClient#STATE_VERSION}) is good; otherwise returns state problems. */
  protected static Map<String, String> checkStateVersionsAreValid(CoreContainer cores, List<String> collectionsList, String stateVer) {
    Map<String, String> result = null;
    String[] pairs;
    if (stateVer != null && !stateVer.isEmpty() && cores.isZooKeeperAware()) {
      // many have multiple collections separated by |
      pairs = StringUtils.split(stateVer, '|');
      for (String pair : pairs) {
        String[] pcs = StringUtils.split(pair, ':');
        if (pcs.length == 2 && !pcs[0].isEmpty() && !pcs[1].isEmpty()) {

          String[] versionAndUpdatesHash = pcs[1].split(">");
          int version = Integer.parseInt(versionAndUpdatesHash[0]);
          int updateHash = Integer.parseInt(versionAndUpdatesHash[1]);

          String status = cores.getZkController().getZkStateReader().compareStateVersions(pcs[0], version, updateHash);
          if (status != null) {
            if (result == null) result = new HashMap<>();
            result.put(pcs[0], status);
          }
        }
      }
    }
    return result;
  }

  static final String CONNECTION_HEADER = "Connection";
  static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";
  static final String CONTENT_LENGTH_HEADER = "Content-Length";

  protected static Set<String> findConnectionHeaders(@Nonnull HttpServletRequest clientRequest)
  {
    // Any header listed by the Connection header must be removed:
    // http://tools.ietf.org/html/rfc7230#section-6.1.
    Set<String> hopHeaders = null;
    Enumeration<String> connectionHeaders = clientRequest.getHeaders(HttpHeader.CONNECTION.asString());
    while (connectionHeaders.hasMoreElements())
    {
      String value = connectionHeaders.nextElement();
      String[] values = value.split(",");
      for (String name : values)
      {
        name = name.trim().toLowerCase(Locale.ENGLISH);
        if (hopHeaders == null)
          hopHeaders = new HashSet<>();
        hopHeaders.add(name);
      }
    }
    return hopHeaders;
  }


  protected static final Set<String> HOP_HEADERS;
  static
  {
    //      hopHeaders.add(HttpHeader.X_FORWARDED_FOR.asString());
    //      hopHeaders.add(HttpHeader.X_FORWARDED_PROTO.asString());
    //      hopHeaders.add(HttpHeader.VIA.asString());
    //      hopHeaders.add(HttpHeader.X_FORWARDED_HOST.asString());
    //      hopHeaders.add(HttpHeader.SERVER.asString());
    //
    HOP_HEADERS = Set.of("accept-encoding", "connection", "keep-alive", "proxy-authorization", "proxy-authenticate", "proxy-connection", "transfer-encoding", "te", "trailer", "upgrade");
  }
}

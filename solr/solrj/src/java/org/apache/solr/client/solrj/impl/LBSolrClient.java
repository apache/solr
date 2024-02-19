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

import static org.apache.solr.common.params.CommonParams.ADMIN_PATHS;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.URLUtil;
import org.slf4j.MDC;

public abstract class LBSolrClient extends SolrClient {

  // defaults
  protected static final Set<Integer> RETRY_CODES =
      new HashSet<>(Arrays.asList(404, 403, 503, 500));
  private static final int NONSTANDARD_PING_LIMIT =
      5; // number of times we'll ping dead servers not in the server list

  // keys to the maps are currently of the form "http://localhost:8983/solr"
  // which should be equivalent to HttpSolrServer.getBaseURL()
  private final Map<String, EndpointWrapper> aliveServers = new LinkedHashMap<>();
  // access to aliveServers should be synchronized on itself

  protected final Map<String, EndpointWrapper> zombieServers = new ConcurrentHashMap<>();

  // changes to aliveServers are reflected in this array, no need to synchronize
  private volatile EndpointWrapper[] aliveServerList = new EndpointWrapper[0];

  private volatile ScheduledExecutorService aliveCheckExecutor;

  protected long aliveCheckIntervalMillis =
      TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS); // 1 minute between checks
  private final AtomicInteger counter = new AtomicInteger(-1);

  private static final SolrQuery solrQuery = new SolrQuery("*:*");
  protected volatile ResponseParser parser;
  protected volatile RequestWriter requestWriter;

  static {
    solrQuery.setRows(0);
    /*
     * Default sort (if we don't supply a sort) is by score and since we request 0 rows any sorting
     * and scoring is not necessary. SolrQuery.DOCID schema-independently specifies a non-scoring
     * sort. <code>_docid_ asc</code> sort is efficient, <code>_docid_ desc</code> sort is not, so
     * choose ascending DOCID sort.
     */
    solrQuery.setSort(SolrQuery.DOCID, SolrQuery.ORDER.asc);
    // not a top-level request, we are interested only in the server being sent to i.e. it need not
    // distribute our request to further servers
    solrQuery.setDistrib(false);
  }

  public static class Endpoint {
    private final String baseUrl;
    private final String core;

    public Endpoint(String baseUrl) {
      this(baseUrl, null);
    }

    public Endpoint(String baseUrl, String core) {
      this.baseUrl = normalize(baseUrl);
      this.core = core;
    }

    public String getBaseUrl() {
      return baseUrl;
    }

    public String getCore() {
      return core;
    }

    @Override
    public String toString() {
      if (core == null) {
        return baseUrl;
      }
      return baseUrl + "/" + core;
    }

    @Override
    public int hashCode() {
      return Objects.hash(baseUrl, core);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof Endpoint)) return false;
      final Endpoint rhs = (Endpoint) obj;

      return Objects.equals(baseUrl, rhs.baseUrl) && Objects.equals(core, rhs.core);
    }

    public static Endpoint from(String unknownUrl) {
      if (URLUtil.isBaseUrl(unknownUrl)) {
        return new Endpoint(unknownUrl);
      }
      return new Endpoint(
          URLUtil.extractBaseUrl(unknownUrl), URLUtil.extractCoreFromCoreUrl(unknownUrl));
    }
  }

  protected static class EndpointWrapper {
    final Endpoint endpoint;

    // "standard" endpoints are used by default.  They normally live in the alive list
    // and move to the zombie list when unavailable.  When they become available again,
    // they move back to the alive list.
    boolean standard = true;

    int failedPings = 0;

    EndpointWrapper(Endpoint endpoint) {
      this.endpoint = endpoint;
    }

    public Endpoint getEndpoint() {
      return endpoint;
    }

    // TODO NOCOMMIT - we probably want 'getBaseUrl' and 'getCore' helpers here, but I'm going to
    // delete them as a way to make sure that I don't miss auditing any usages to make sure that
    // they're accounting for the core/baseUrl correctly.

    @Override
    public String toString() {
      return endpoint.toString();
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof EndpointWrapper)) return false;
      return endpoint.equals(((EndpointWrapper) obj).getEndpoint());
    }
  }

  protected static class EndpointIterator {
    Endpoint endpoint;
    List<Endpoint> skipped;
    int numServersTried;
    Iterator<Endpoint> it;
    Iterator<Endpoint> skippedIt;
    String exceptionMessage;
    long timeAllowedNano;
    long timeOutTime;

    final Map<String, EndpointWrapper> zombieServers;
    final Req req;

    public EndpointIterator(Req req, Map<String, EndpointWrapper> zombieServers) {
      this.it = req.getEndpoints().iterator();
      this.req = req;
      this.zombieServers = zombieServers;
      this.timeAllowedNano = getTimeAllowedInNanos(req.getRequest());
      this.timeOutTime = System.nanoTime() + timeAllowedNano;
      fetchNext();
    }

    public synchronized boolean hasNext() {
      return endpoint != null;
    }

    private void fetchNext() {
      endpoint = null;
      if (req.numServersToTry != null && numServersTried > req.numServersToTry) {
        exceptionMessage = "Time allowed to handle this request exceeded";
        return;
      }

      while (it.hasNext()) {
        endpoint = it.next();
        // if the server is currently a zombie, just skip to the next one
        EndpointWrapper wrapper = zombieServers.get(endpoint);
        if (wrapper != null) {
          final int numDeadServersToTry = req.getNumDeadServersToTry();
          if (numDeadServersToTry > 0) {
            if (skipped == null) {
              skipped = new ArrayList<>(numDeadServersToTry);
              skipped.add(wrapper.getEndpoint());
            } else if (skipped.size() < numDeadServersToTry) {
              skipped.add(wrapper.getEndpoint());
            }
          }
          continue;
        }

        break;
      }
      if (endpoint == null && skipped != null) {
        if (skippedIt == null) {
          skippedIt = skipped.iterator();
        }
        if (skippedIt.hasNext()) {
          endpoint = skippedIt.next();
        }
      }
    }

    boolean isServingZombieServer() {
      return skippedIt != null;
    }

    public synchronized Endpoint nextOrError() throws SolrServerException {
      return nextOrError(null);
    }

    public synchronized Endpoint nextOrError(Exception previousEx) throws SolrServerException {
      String suffix = "";
      if (previousEx == null) {
        suffix = ":" + zombieServers.keySet();
      }
      // Skipping check time exceeded for the first request
      if (numServersTried > 0 && isTimeExceeded(timeAllowedNano, timeOutTime)) {
        throw new SolrServerException(
            "Time allowed to handle this request exceeded" + suffix, previousEx);
      }
      if (endpoint == null) {
        throw new SolrServerException(
            "No live SolrServers available to handle this request" + suffix, previousEx);
      }
      numServersTried++;
      if (req.getNumServersToTry() != null && numServersTried > req.getNumServersToTry()) {
        throw new SolrServerException(
            "No live SolrServers available to handle this request:"
                + " numServersTried="
                + numServersTried
                + " numServersToTry="
                + req.getNumServersToTry()
                + suffix,
            previousEx);
      }
      Endpoint rs = endpoint;
      fetchNext();
      return rs;
    }
  }

  // Req should be parameterized too, but that touches a whole lotta code
  public static class Req {
    protected SolrRequest<?> request;
    protected List<Endpoint> endpoints;
    protected int numDeadServersToTry;
    private final Integer numServersToTry;

    public Req(SolrRequest<?> request, List<Endpoint> endpoints) {
      this(request, endpoints, null);
    }

    public Req(SolrRequest<?> request, List<Endpoint> endpoints, Integer numServersToTry) {
      this.request = request;
      this.endpoints = endpoints;
      this.numDeadServersToTry = endpoints.size();
      this.numServersToTry = numServersToTry;
    }

    public SolrRequest<?> getRequest() {
      return request;
    }

    public List<Endpoint> getEndpoints() {
      return endpoints;
    }

    /**
     * @return the number of dead servers to try if there are no live servers left
     */
    public int getNumDeadServersToTry() {
      return numDeadServersToTry;
    }

    /**
     * @param numDeadServersToTry The number of dead servers to try if there are no live servers
     *     left. Defaults to the number of servers in this request.
     */
    public void setNumDeadServersToTry(int numDeadServersToTry) {
      this.numDeadServersToTry = numDeadServersToTry;
    }

    public Integer getNumServersToTry() {
      return numServersToTry;
    }
  }

  public static class Rsp {
    protected String server;
    protected NamedList<Object> rsp;

    /** The response from the server */
    public NamedList<Object> getResponse() {
      return rsp;
    }

    /**
     * The server URL that returned the response
     *
     * <p>May be either a true "base URL" or a core/collection URL, depending on the request
     */
    public String getServer() {
      return server;
    }
  }

  public LBSolrClient(List<Endpoint> solrEndpoints) {
    if (!solrEndpoints.isEmpty()) {
      for (Endpoint s : solrEndpoints) {
        EndpointWrapper wrapper = createServerWrapper(s);
        aliveServers.put(wrapper.getEndpoint().toString(), wrapper);
      }
      updateAliveList();
    }
  }

  protected void updateAliveList() {
    synchronized (aliveServers) {
      aliveServerList = aliveServers.values().toArray(new EndpointWrapper[0]);
    }
  }

  protected EndpointWrapper createServerWrapper(Endpoint baseUrl) {
    return new EndpointWrapper(baseUrl);
  }

  public static String normalize(String server) {
    if (server.endsWith("/")) server = server.substring(0, server.length() - 1);
    return server;
  }

  /**
   * Tries to query a live server from the list provided in Req. Servers in the dead pool are
   * skipped. If a request fails due to an IOException, the server is moved to the dead pool for a
   * certain period of time, or until a test request on that server succeeds.
   *
   * <p>Servers are queried in the exact order given (except servers currently in the dead pool are
   * skipped). If no live servers from the provided list remain to be tried, a number of previously
   * skipped dead servers will be tried. Req.getNumDeadServersToTry() controls how many dead servers
   * will be tried.
   *
   * <p>If no live servers are found a SolrServerException is thrown.
   *
   * @param req contains both the request as well as the list of servers to query
   * @return the result of the request
   * @throws IOException If there is a low-level I/O error.
   */
  public Rsp request(Req req) throws SolrServerException, IOException {
    Rsp rsp = new Rsp();
    Exception ex = null;
    boolean isNonRetryable =
        req.request instanceof IsUpdateRequest || ADMIN_PATHS.contains(req.request.getPath());
    EndpointIterator endpointIterator = new EndpointIterator(req, zombieServers);
    Endpoint serverStr;
    while ((serverStr = endpointIterator.nextOrError(ex)) != null) {
      try {
        MDC.put("LBSolrClient.url", serverStr.toString());
        ex =
            doRequest(
                serverStr, req, rsp, isNonRetryable, endpointIterator.isServingZombieServer());
        if (ex == null) {
          return rsp; // SUCCESS
        }
      } finally {
        MDC.remove("LBSolrClient.url");
      }
    }
    throw new SolrServerException(
        "No live SolrServers available to handle this request. (Tracking "
            + zombieServers.size()
            + " not live)",
        ex);
  }

  /**
   * @return time allowed in nanos, returns -1 if no time_allowed is specified.
   */
  private static long getTimeAllowedInNanos(final SolrRequest<?> req) {
    SolrParams reqParams = req.getParams();
    return reqParams == null
        ? -1
        : TimeUnit.NANOSECONDS.convert(
            reqParams.getInt(CommonParams.TIME_ALLOWED, -1), TimeUnit.MILLISECONDS);
  }

  private static boolean isTimeExceeded(long timeAllowedNano, long timeOutTime) {
    return timeAllowedNano > 0 && System.nanoTime() > timeOutTime;
  }

  protected Exception doRequest(
      Endpoint baseUrl, Req req, Rsp rsp, boolean isNonRetryable, boolean isZombie)
      throws SolrServerException, IOException {
    Exception ex = null;
    try {
      rsp.server = baseUrl.toString();
      req.getRequest().setBasePath(baseUrl.toString());
      rsp.rsp = getClient(baseUrl).request(req.getRequest(), (String) null);
      if (isZombie) {
        zombieServers.remove(baseUrl);
      }
    } catch (BaseHttpSolrClient.RemoteExecutionException e) {
      throw e;
    } catch (SolrException e) {
      // we retry on 404 or 403 or 503 or 500
      // unless it's an update - then we only retry on connect exception
      if (!isNonRetryable && RETRY_CODES.contains(e.code())) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else {
        // Server is alive but the request was likely malformed or invalid
        if (isZombie) {
          zombieServers.remove(baseUrl.toString());
        }
        throw e;
      }
    } catch (SocketException e) {
      if (!isNonRetryable || e instanceof ConnectException) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else {
        throw e;
      }
    } catch (SocketTimeoutException e) {
      if (!isNonRetryable) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else {
        throw e;
      }
    } catch (SolrServerException e) {
      Throwable rootCause = e.getRootCause();
      if (!isNonRetryable && rootCause instanceof IOException) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else if (isNonRetryable && rootCause instanceof ConnectException) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else {
        throw e;
      }
    } catch (Exception e) {
      throw new SolrServerException(e);
    }

    return ex;
  }

  protected abstract SolrClient getClient(Endpoint endpoint);

  protected Exception addZombie(Endpoint serverStr, Exception e) {
    EndpointWrapper wrapper = createServerWrapper(serverStr);
    wrapper.standard = false;
    zombieServers.put(serverStr.toString(), wrapper);
    startAliveCheckExecutor();
    return e;
  }

  private void startAliveCheckExecutor() {
    // double-checked locking, but it's OK because we don't *do* anything with aliveCheckExecutor
    // if it's not null.
    if (aliveCheckExecutor == null) {
      synchronized (this) {
        if (aliveCheckExecutor == null) {
          aliveCheckExecutor =
              Executors.newSingleThreadScheduledExecutor(
                  new SolrNamedThreadFactory("aliveCheckExecutor"));
          aliveCheckExecutor.scheduleAtFixedRate(
              getAliveCheckRunner(new WeakReference<>(this)),
              this.aliveCheckIntervalMillis,
              this.aliveCheckIntervalMillis,
              TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  private static Runnable getAliveCheckRunner(final WeakReference<LBSolrClient> lbRef) {
    return () -> {
      LBSolrClient lb = lbRef.get();
      if (lb != null && lb.zombieServers != null) {
        for (Object zombieServer : lb.zombieServers.values()) {
          lb.checkAZombieServer((EndpointWrapper) zombieServer);
        }
      }
    };
  }

  public ResponseParser getParser() {
    return parser;
  }

  public RequestWriter getRequestWriter() {
    return requestWriter;
  }

  private void checkAZombieServer(EndpointWrapper zombieServer) {
    final Endpoint zombieEndpoint = zombieServer.getEndpoint();
    try {
      QueryRequest queryRequest = new QueryRequest(solrQuery);
      queryRequest.setBasePath(zombieEndpoint.toString());
      QueryResponse resp = queryRequest.process(getClient(zombieEndpoint));
      if (resp.getStatus() == 0) {
        // server has come back up.
        // make sure to remove from zombies before adding to alive to avoid a race condition
        // where another thread could mark it down, move it back to zombie, and then we delete
        // from zombie and lose it forever.
        EndpointWrapper wrapper = zombieServers.remove(zombieServer.getEndpoint().toString());
        if (wrapper != null) {
          wrapper.failedPings = 0;
          if (wrapper.standard) {
            addToAlive(wrapper);
          }
        } else {
          // something else already moved the server from zombie to alive
        }
      }
    } catch (Exception e) {
      // Expected. The server is still down.
      zombieServer.failedPings++;

      // If the server doesn't belong in the standard set belonging to this load balancer
      // then simply drop it after a certain number of failed pings.
      if (!zombieServer.standard && zombieServer.failedPings >= NONSTANDARD_PING_LIMIT) {
        zombieServers.remove(zombieEndpoint.toString());
      }
    }
  }

  private EndpointWrapper removeFromAlive(String key) {
    synchronized (aliveServers) {
      EndpointWrapper wrapper = aliveServers.remove(key);
      if (wrapper != null) updateAliveList();
      return wrapper;
    }
  }

  private void addToAlive(EndpointWrapper wrapper) {
    synchronized (aliveServers) {
      EndpointWrapper prev = aliveServers.put(wrapper.getEndpoint().getBaseUrl(), wrapper);
      // TODO: warn if there was a previous entry?
      updateAliveList();
    }
  }

  public String removeSolrServer(String server) {
    try {
      server = new URL(server).toExternalForm();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
    if (server.endsWith("/")) {
      server = server.substring(0, server.length() - 1);
    }

    // there is a small race condition here - if the server is in the process of being moved between
    // lists, we could fail to remove it.
    removeFromAlive(server);
    zombieServers.remove(server);
    return null;
  }

  /**
   * Tries to query a live server. A SolrServerException is thrown if all servers are dead. If the
   * request failed due to IOException then the live server is moved to dead pool and the request is
   * retried on another live server. After live servers are exhausted, any servers previously marked
   * as dead will be tried before failing the request.
   *
   * @param request the SolrRequest.
   * @return response
   * @throws IOException If there is a low-level I/O error.
   */
  @Override
  public NamedList<Object> request(final SolrRequest<?> request, String collection)
      throws SolrServerException, IOException {
    return request(request, collection, null);
  }

  public NamedList<Object> request(
      final SolrRequest<?> request, String collection, final Integer numServersToTry)
      throws SolrServerException, IOException {
    Exception ex = null;
    EndpointWrapper[] serverList = aliveServerList;

    final int maxTries = (numServersToTry == null ? serverList.length : numServersToTry.intValue());
    int numServersTried = 0;
    Map<String, EndpointWrapper> justFailed = null;
    if (ClientUtils.shouldApplyDefaultCollection(collection, request))
      collection = defaultCollection;

    boolean timeAllowedExceeded = false;
    long timeAllowedNano = getTimeAllowedInNanos(request);
    long timeOutTime = System.nanoTime() + timeAllowedNano;
    for (int attempts = 0; attempts < maxTries; attempts++) {
      timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime);
      if (timeAllowedExceeded) {
        break;
      }

      EndpointWrapper wrapper = pickServer(serverList, request);
      final var endpoint = wrapper.getEndpoint();
      try {
        ++numServersTried;
        request.setBasePath(endpoint.toString());
        return getClient(endpoint).request(request, collection);
      } catch (SolrException e) {
        // Server is alive but the request was malformed or invalid
        throw e;
      } catch (SolrServerException e) {
        if (e.getRootCause() instanceof IOException) {
          ex = e;
          moveAliveToDead(wrapper);
          if (justFailed == null) justFailed = new HashMap<>();
          justFailed.put(endpoint.toString(), wrapper);
        } else {
          throw e;
        }
      } catch (Exception e) {
        throw new SolrServerException(e);
      }
    }

    // try other standard servers that we didn't try just now
    for (EndpointWrapper wrapper : zombieServers.values()) {
      final var endpoint = wrapper.getEndpoint();
      timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime);
      if (timeAllowedExceeded) {
        break;
      }

      if (wrapper.standard == false
          || (justFailed != null && justFailed.containsKey(endpoint.toString()))) continue;
      try {
        ++numServersTried;
        request.setBasePath(endpoint.toString());
        NamedList<Object> rsp = getClient(endpoint).request(request, collection);
        // remove from zombie list *before* adding to alive to avoid a race that could lose a server
        zombieServers.remove(endpoint.toString());
        addToAlive(wrapper);
        return rsp;
      } catch (SolrException e) {
        // Server is alive but the request was malformed or invalid
        throw e;
      } catch (SolrServerException e) {
        if (e.getRootCause() instanceof IOException) {
          ex = e;
          // still dead
        } else {
          throw e;
        }
      } catch (Exception e) {
        throw new SolrServerException(e);
      }
    }

    final String solrServerExceptionMessage;
    if (timeAllowedExceeded) {
      solrServerExceptionMessage = "Time allowed to handle this request exceeded";
    } else {
      if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
        solrServerExceptionMessage =
            "No live SolrServers available to handle this request:"
                + " numServersTried="
                + numServersTried
                + " numServersToTry="
                + numServersToTry.intValue();
      } else {
        solrServerExceptionMessage = "No live SolrServers available to handle this request";
      }
    }
    if (ex == null) {
      throw new SolrServerException(solrServerExceptionMessage);
    } else {
      throw new SolrServerException(solrServerExceptionMessage, ex);
    }
  }

  /**
   * Pick a server from list to execute request. By default servers are picked in round-robin
   * manner, custom classes can override this method for more advance logic
   *
   * @param aliveServerList list of currently alive servers
   * @param request the request will be sent to the picked server
   * @return the picked server
   */
  protected EndpointWrapper pickServer(EndpointWrapper[] aliveServerList, SolrRequest<?> request) {
    int count = counter.incrementAndGet() & Integer.MAX_VALUE;
    return aliveServerList[count % aliveServerList.length];
  }

  private void moveAliveToDead(EndpointWrapper wrapper) {
    wrapper = removeFromAlive(wrapper.getEndpoint().toString());
    if (wrapper == null) return; // another thread already detected the failure and removed it
    zombieServers.put(wrapper.getEndpoint().toString(), wrapper);
    startAliveCheckExecutor();
  }

  @Override
  public void close() {
    synchronized (this) {
      if (aliveCheckExecutor != null) {
        aliveCheckExecutor.shutdownNow();
        ExecutorUtil.shutdownAndAwaitTermination(aliveCheckExecutor);
      }
    }
  }
}

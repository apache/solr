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

import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.DISABLE;
import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.ENABLE;
import static org.apache.solr.core.RequestParams.USEPARAM;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Locale;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ping Request Handler for reporting SolrCore health to a Load Balancer.
 *
 * <p>This handler is designed to be used as the endpoint for an HTTP Load-Balancer to use when
 * checking the "health" or "up status" of a Solr server.
 *
 * <p>In its simplest form, the PingRequestHandler should be configured with some defaults
 * indicating a request that should be executed. If the request succeeds, then the
 * PingRequestHandler will respond back with a simple "OK" status. If the request fails, then the
 * PingRequestHandler will respond back with the corresponding HTTP Error code. Clients (such as
 * load balancers) can be configured to poll the PingRequestHandler monitoring for these types of
 * responses (or for a simple connection failure) to know if there is a problem with the Solr
 * server.
 *
 * <p>A distributed ping is fanned out to the delegated handler on each shard (by default the
 * /select handler).
 *
 * <pre class="prettyprint">
 * &lt;requestHandler name="/admin/ping" class="solr.PingRequestHandler"&gt;
 *   &lt;lst name="invariants"&gt;
 *     &lt;str name="qt"&gt;/search&lt;/str&gt;&lt;!-- handler to delegate to --&gt;
 *     &lt;str name="q"&gt;some test query&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/requestHandler&gt;
 * </pre>
 *
 * <p>A more advanced option available, is to configure the handler with a "healthcheckFile" which
 * can be used to enable/disable the PingRequestHandler.
 *
 * <pre class="prettyprint">
 * &lt;requestHandler name="/admin/ping" class="solr.PingRequestHandler"&gt;
 *   &lt;!-- relative paths are resolved against the data dir --&gt;
 *   &lt;str name="healthcheckFile"&gt;server-enabled.txt&lt;/str&gt;
 *   &lt;lst name="invariants"&gt;
 *     &lt;str name="qt"&gt;/search&lt;/str&gt;&lt;!-- handler to delegate to --&gt;
 *     &lt;str name="q"&gt;some test query&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/requestHandler&gt;
 * </pre>
 *
 * <ul>
 *   <li>If the health check file exists, the handler will execute the delegated query and return
 *       status as described above.
 *   <li>If the health check file does not exist, the handler will return an HTTP error even if the
 *       server is working fine and the delegated query would have succeeded
 * </ul>
 *
 * <p>This health check file feature can be used as a way to indicate to some Load Balancers that
 * the server should be "removed from rotation" for maintenance, or upgrades, or whatever reason you
 * may wish.
 *
 * <p>The health check file may be created/deleted by any external system, or the PingRequestHandler
 * itself can be used to create/delete the file by specifying an "action" param in a request:
 *
 * <ul>
 *   <li><code>http://.../ping?action=enable</code> - creates the health check file if it does not
 *       already exist
 *   <li><code>http://.../ping?action=disable</code> - deletes the health check file if it exists
 *   <li><code>http://.../ping?action=status</code> - returns a status code indicating if the
 *       healthcheck file exists ("<code>enabled</code>") or not ("<code>disabled</code>")
 * </ul>
 *
 * @deprecated This handler is deprecated and will be removed in a future release. For load balancer
 *     or orchestration health checks, use {@link org.apache.solr.handler.admin.HealthCheckHandler}
 *     instead. instead.
 * @since solr 1.3
 */
@Deprecated(since = "10.1")
public class PingRequestHandler extends RequestHandlerBase implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String HEALTHCHECK_FILE_PARAM = "healthcheckFile";

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    String action = request.getParams().get(ACTION, "").strip().toLowerCase(Locale.ROOT);
    // Modifying the health check file requires more permission than just doing a ping
    switch (action) {
      case ENABLE:
      case DISABLE:
        return Name.CONFIG_EDIT_PERM;
      default:
        return Name.HEALTH_PERM;
    }
  }

  protected enum ACTIONS {
    STATUS,
    ENABLE,
    DISABLE,
    PING
  };

  private String healthFileName = null;
  private Path healthcheck = null;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    Object tmp = args.get(HEALTHCHECK_FILE_PARAM);
    healthFileName = (null == tmp ? null : tmp.toString());
  }

  @Override
  public void inform(SolrCore core) {
    if (null != healthFileName) {
      healthcheck = Path.of(healthFileName);
      if (!healthcheck.isAbsolute()) {
        healthcheck = Path.of(core.getDataDir(), healthFileName);
        healthcheck = healthcheck.toAbsolutePath();
      }

      if (!Files.isWritable(healthcheck.getParent())) {
        // this is not fatal, users may not care about enable/disable via
        // solr request, file might be touched/deleted by an external system
        log.warn(
            "Directory for configured healthcheck file is not writable by solr, PingRequestHandler will not be able to control enable/disable: {}",
            healthcheck.getParent().toAbsolutePath());
      }
    }
  }

  /**
   * Returns true if the healthcheck flag-file is enabled but does not exist, otherwise (no file
   * configured, or file configured and exists) returns false.
   */
  public boolean isPingDisabled() {
    return (null != healthcheck && !Files.exists(healthcheck));
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

    SolrParams params = req.getParams();

    // in this case, we want to default distrib to false so
    // we only ping the single node
    Boolean distrib = params.getBool(DISTRIB);
    if (distrib == null) {
      ModifiableSolrParams mparams = new ModifiableSolrParams(params);
      mparams.set(DISTRIB, false);
      req.setParams(mparams);
    }

    String actionParam = params.get("action");
    ACTIONS action = null;
    if (actionParam == null) {
      action = ACTIONS.PING;
    } else {
      try {
        action = ACTIONS.valueOf(actionParam.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException iae) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + actionParam);
      }
    }
    switch (action) {
      case PING:
        if (isPingDisabled()) {
          SolrException e =
              new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Service disabled");
          rsp.setException(e);
          return;
        }
        handlePing(req, rsp);
        break;
      case ENABLE:
        handleEnable(true);
        break;
      case DISABLE:
        handleEnable(false);
        break;
      case STATUS:
        if (healthcheck == null) {
          SolrException e =
              new SolrException(
                  SolrException.ErrorCode.SERVICE_UNAVAILABLE, "healthcheck not configured");
          rsp.setException(e);
        } else {
          rsp.add("status", isPingDisabled() ? "disabled" : "enabled");
        }
    }
  }

  protected void handlePing(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

    SolrCore core = req.getCore();

    SolrParams configParams = resolveConfiguredParams(req);
    String qt = configParams.get(CommonParams.QT);
    SolrRequestHandler handler = core.getRequestHandler(qt);
    if (handler == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Unknown RequestHandler (qt): " + qt);
    }
    if (handler instanceof PingRequestHandler) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Cannot execute the PingRequestHandler recursively");
    }

    ModifiableSolrParams overrides = new ModifiableSolrParams();
    boolean distrib = req.getParams().getBool(DISTRIB, false);
    overrides.set(DISTRIB, distrib);
    if (distrib) {
      // target the delegate on each shard, not this ping handler
      overrides.set(ShardParams.SHARDS_QT, qt == null ? "/select" : qt);
    }

    // Execute the ping query and catch any possible exception
    Throwable ex = null;
    try (SolrQueryRequest pingReq =
        req.subRequest(SolrParams.wrapDefaults(overrides, configParams))) {
      SolrQueryResponse pingrsp = new SolrQueryResponse();
      core.execute(handler, pingReq, pingrsp);
      ex = pingrsp.getException();
      NamedList<Object> headers = rsp.getResponseHeader();
      if (headers != null) {
        headers.add("zkConnected", pingrsp.getResponseHeader().get("zkConnected"));
      }
    } catch (Exception e) {
      ex = e;
    }

    // Send an error or an 'OK' message (response code will be 200)
    if (ex != null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Ping query caused exception: " + ex.getMessage(),
          ex);
    }

    rsp.add("status", "OK");
  }

  /**
   * Resolves this handler's configured invariants, appends, defaults and {@code useParams}
   * paramsets into a single {@link SolrParams}. The delegate handler is named by {@code qt}; a null
   * {@code qt} means the core's default handler.
   */
  private SolrParams resolveConfiguredParams(SolrQueryRequest req) {
    try (SolrQueryRequest configOnly = req.subRequest(new ModifiableSolrParams())) {
      PluginInfo info = getPluginInfo();
      if (info != null && info.attributes.containsKey(USEPARAM)) {
        configOnly.getContext().put(USEPARAM, info.attributes.get(USEPARAM));
      }
      SolrPluginUtils.setDefaults(configOnly, defaults, appends, invariants);
      return configOnly.getParams();
    }
  }

  protected void handleEnable(boolean enable) throws SolrException {
    if (healthcheck == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVICE_UNAVAILABLE, "No healthcheck file defined.");
    }
    if (enable) {
      try {
        // write out when the file was created
        Files.write(healthcheck, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Unable to write healthcheck flag file", e);
      }
    } else {
      try {
        Files.deleteIfExists(healthcheck);
      } catch (Throwable cause) {
        throw new SolrException(
            SolrException.ErrorCode.NOT_FOUND,
            "Did not successfully delete healthcheck file: " + healthcheck.toAbsolutePath(),
            cause);
      }
    }
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Reports application health to a load-balancer";
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}

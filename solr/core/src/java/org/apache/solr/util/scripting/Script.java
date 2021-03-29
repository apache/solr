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

package org.apache.solr.util.scripting;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.PropertiesUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class represents a script consisting of a series of operations.
 */
public class Script implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String INIT_SCRIPT = "initScript";
  public static final String SHUTDOWN_SCRIPT = "shutdownScript";


  /** Context variable: Random live node name. */
  public static final String RANDOM_NODE_CTX_PROP = "_random_node_";
  /** Context variable: My node name, if applicable. */
  public static final String NODE_NAME_CTX_PROP = "_node_name_";
  /** Context variable: List of live nodes. */
  public static final String LIVE_NODES_CTX_PROP = "_live_nodes_";
  /** Context variable: List of collections. */
  public static final String COLLECTIONS_CTX_PROP = "_collections_";
  /** Context variable: List of SolrResponses of SOLR_REQUEST operations. */
  public static final String RESPONSES_CTX_PROP = "_responses_";
  /** Context variable: Current loop iteration or none if outside of loop. */
  public static final String LOOP_ITER_PROP = "_loop_iter_";
  /** Context variable: Timeout for operation to complete, in ms. */
  public static final String TIMEOUT_PROP = "_timeout_";
  /** Context variable: How to handle errors. */
  public static final String ERROR_HANDLING_PROP = "errorHandling";

  public enum ErrorHandling {
    IGNORE,
    END_SCRIPT,
    FATAL;

    public static ErrorHandling get(String str) {
      if (str != null) {
        try {
          return ErrorHandling.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          return null;
        }
      } else {
        return null;
      }
    }
  }

  public static final int DEFAULT_OP_TIMEOUT_MS = 30000;

  public final List<ScriptOp> ops = new ArrayList<>();
  public final Map<String, Object> context = new HashMap<>();
  public final CloudSolrClient client;
  public final String nodeName;
  public final SolrCloudManager cloudManager;
  public final TimeSource timeSource = TimeSource.NANO_TIME;
  public PrintStream console = System.err;
  public boolean verbose;
  public boolean abortLoop;
  public boolean abortScript;
  public ErrorHandling errorHandling = ErrorHandling.END_SCRIPT;
  public final Random random = new Random();

  /** Base class for implementation of script DSL ScriptActions. */
  public static abstract class ScriptOp {
    ModifiableSolrParams initParams;
    ModifiableSolrParams params;

    public void init(SolrParams params) {
      this.initParams = new ModifiableSolrParams(params);
    }

    /**
     * This method prepares a copy of initial params (and sets the value of {@link #params}
     * with all property references resolved against the current {@link Script#context}
     * and system properties. This method should always be called before invoking
     * {@link #execute(Script)}.
     * @param script current script
     */
    @SuppressWarnings({"unchecked"})
    public void prepareCurrentParams(Script script) {
      Properties props = new Properties();
      script.context.forEach((k, v) -> {
        if (v instanceof String[]) {
          v = String.join(",", (String[]) v);
        } else if (v instanceof Collection) {
          StringBuilder sb = new StringBuilder();
          for (Object o : (Collection<Object>)v) {
            if (sb.length() > 0) {
              sb.append(',');
            }
            if ((o instanceof String) || (o instanceof Number)) {
              sb.append(o);
            } else {
              // skip all values
              return;
            }
          }
          v = sb.toString();
        } else if ((v instanceof String)) {
          // don't convert, put as is
        } else if ((v instanceof Number)) {
          v = v.toString();
        } else {
          // skip
          return;
        }
        props.put(k, v);
      });
      ModifiableSolrParams currentParams = new ModifiableSolrParams();
      initParams.forEach(e -> {
        String newKey = PropertiesUtil.substituteProperty(e.getKey(), props);
        if (newKey == null) {
          newKey = e.getKey();
        }
        String[] newValues;
        if (e.getValue() != null && e.getValue().length > 0) {
          String[] values = e.getValue();
          newValues = new String[values.length];
          for (int k = 0; k < values.length; k++) {
            String newVal = PropertiesUtil.substituteProperty(values[k], props);
            if (newVal == null) {
              newVal = values[k];
            }
            newValues[k] = newVal;
          }
        } else {
          newValues = e.getValue();
        }
        currentParams.add(newKey, newValues);
      });
      params = currentParams;
    }

    /**
     * Execute the operation.
     * @param script current script.
     */
    public abstract void execute (Script script) throws Exception;

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + "{" + (params != null ? params : initParams) + "}";
    }
  }


  /**
   * Actions supported by the script.
   */
  public enum ScriptAction {
    /** Start a loop. */
    LOOP_START,
    /** End a loop. */
    LOOP_END,
    /** Execute a SolrRequest. */
    SOLR_REQUEST,
    /** Wait for a collection to reach the indicated number of shards and replicas. */
    WAIT_COLLECTION,
    /** Prepare a listener to listen for an autoscaling event. */
    EVENT_LISTENER,
    /** Wait for an autoscaling event using previously prepared listener. */
    WAIT_EVENT,
    /** Wait for a while, allowing background tasks to execute. */
    WAIT,
    /** Set a variable in context. */
    SET,
    /** Remove a variable from context. */
    CLEAR,
    /** Assert a condition. */
    ASSERT,
    /** Set an operation timeout. */
    TIMEOUT,
    /** Dump the script and the current context to the log. */
    DUMP;

    public static ScriptAction get(String str) {
      if (str != null) {
        try {
          return ScriptAction.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          return null;
        }
      } else {
        return null;
      }
    }

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }
  }

  public static Map<ScriptAction, Class<? extends ScriptOp>> supportedOps = new HashMap<>();
  static {
    supportedOps.put(ScriptAction.LOOP_START, LoopOp.class);
    supportedOps.put(ScriptAction.LOOP_END, null);
    supportedOps.put(ScriptAction.SOLR_REQUEST, RunSolrRequest.class);
    supportedOps.put(ScriptAction.WAIT, Wait.class);
    supportedOps.put(ScriptAction.WAIT_COLLECTION, WaitCollection.class);
    supportedOps.put(ScriptAction.SET, CtxSet.class);
    supportedOps.put(ScriptAction.CLEAR, CtxClear.class);
    supportedOps.put(ScriptAction.ASSERT, Assert.class);
    supportedOps.put(ScriptAction.TIMEOUT, Timeout.class);
    supportedOps.put(ScriptAction.DUMP, Dump.class);
  }
  
  public Script(CloudSolrClient client, String nodeName) {
    this.client = client;
    this.nodeName = nodeName;
    this.cloudManager = new SolrClientCloudManager(null, client);
    // We make things reproducible in tests by using test seed if any
    String seed = System.getProperty("tests.seed");
    if (seed != null) {
      random.setSeed(seed.hashCode());
    }
  }

  /**
   * Loop ScriptAction.
   */
  public static class LoopOp extends ScriptOp {
    // populated by the DSL parser
    List<ScriptOp> ops = new ArrayList<>();
    int iterations;

    @Override
    public void execute(Script script) throws Exception {
      iterations = Integer.parseInt(params.get("iterations", "10"));
      for (int i = 0; i < iterations; i++) {
        if (script.abortLoop) {
          log.info("        -- abortLoop requested, aborting after {} iterations.", i);
          return;
        }
        script.context.put(LOOP_ITER_PROP, String.valueOf(i));
        log.info("   * iter {} :", i + 1); // logOK
        for (ScriptOp op : ops) {
          op.prepareCurrentParams(script);
          if (log.isInfoEnabled()) {
            log.info("     - {}\t{})", op.getClass().getSimpleName(), op.params);
          }
          op.execute(script);
          if (script.abortLoop) {
            log.info("        -- abortLoop requested, aborting after {} iterations.", i);
            return;
          }
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(this.getClass().getSimpleName() + "{" + params + ", ops=" + ops.size());
      for (int i = 0; i < ops.size(); i++) {
        sb.append("\n    " + (i + 1) + ". " + ops.get(i));
      }
      return sb.toString();
    }
  }

  /**
   * Set a context property.
   */
  public static class CtxSet extends ScriptOp {
    @Override
    public void execute(Script script) throws Exception {
      String key = params.required().get("key");
      String[] values = params.required().getParams("value");
      if (values != null) {
        script.context.put(key, Arrays.asList(values));
      } else {
        script.context.remove(key);
      }
    }
  }

  /**
   * Remove a context property.
   */
  public static class CtxClear extends ScriptOp {
    @Override
    public void execute(Script script) throws Exception {
      String key = params.required().get("key");
      script.context.remove(key);
    }
  }

  /**
   * Dump the script and the current context to log output.
   */
  public static class Dump extends ScriptOp {

    @Override
    public void execute(Script script) throws Exception {
      log.info("========= START script dump ==========");
      log.info("---------    Script Ops     ----------");
      for (int i = 0; i < script.ops.size(); i++) {
        log.info("{}. {}", i + 1, script.ops.get(i));
      }
      log.info("---------  Script Context   ----------");
      TreeMap<String, Object> map = new TreeMap<>(script.context);
      map.forEach((key, value) -> {
        if (value instanceof Collection) {
          @SuppressWarnings("unchecked")
          Collection<Object> collection = (Collection<Object>) value;
          log.info("{} size={}", key, collection.size());
          int i = 1;
          for (Object o : collection) {
            log.info("\t{}.\t{}", i, o);
            i++;
          }
        } else if (value instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> m = (Map<String, Object>) value;
          log.info("{} size={}", key, m.size());
          m.forEach((k, v) -> log.info("\t{}\t{}", k, v));
        } else {
          log.info("{}\t{}", key, value);
        }
      });
      log.info("=========  END script dump  ==========");
    }
  }

  /**
   * Execute a SolrRequest.
   */
  public static class RunSolrRequest extends ScriptOp {
    @Override
    public void execute(Script script) throws Exception {
      String path = params.get("path", "/");
      SolrRequest.METHOD m = SolrRequest.METHOD.valueOf(params.get("httpMethod", "GET"));
      params.remove("httpMethod");
      String streamBody = params.get("stream.body");
      params.remove("stream.body");
      GenericSolrRequest req = new GenericSolrRequest(m, path, params);
      if (streamBody != null) {
        req.setContentWriter(new RequestWriter.StringPayloadContentWriter(streamBody, "application/json"));
      }
      NamedList<Object> rsp = script.client.request(req);
      @SuppressWarnings("unchecked")
      List<NamedList<Object>> responses = (List<NamedList<Object>>) script.context.computeIfAbsent(RESPONSES_CTX_PROP, o -> new ArrayList<NamedList<Object>>());
      responses.add(rsp);
    }
  }

  /**
   * Set (or reset) the timeout for operations to execute.
   */
  public static class Timeout extends ScriptOp {
    @Override
    public void execute(Script script) throws Exception {
      String value = params.get("value", "" + DEFAULT_OP_TIMEOUT_MS);
      long newTimeout = DEFAULT_OP_TIMEOUT_MS;
      if (!value.equals("default")) {
        newTimeout = Long.parseLong(value);
      }
      script.context.put(TIMEOUT_PROP, newTimeout);
    }
  }

  /**
   * Sit idle for a while, 10s by default.
   */
  public static class Wait extends ScriptOp {
    @Override
    public void execute(Script script) throws Exception {
      int timeMs = params.getInt("time", 10000);
      script.timeSource.sleep(timeMs);
    }
  }

  /**
   * Wait for a specific collection shape.
   */
  public static class WaitCollection extends ScriptOp {
    @Override
    public void execute(Script script) throws Exception {
      String collection = params.required().get("collection");
      int shards = Integer.parseInt(params.required().get("shards"));
      int replicas = Integer.parseInt(params.required().get("replicas"));
      boolean withInactive = params.getBool("withInactive", false);
      boolean requireLeaders = params.getBool("requireLeaders", true);
      int waitSec = params.required().getInt("wait", CloudUtil.DEFAULT_TIMEOUT);
      CloudUtil.waitForState(script.cloudManager, collection, waitSec, TimeUnit.SECONDS,
          CloudUtil.clusterShape(shards, replicas, withInactive, requireLeaders));
    }
  }

  public enum Condition {
    EQUALS,
    NOT_EQUALS,
    NULL,
    NOT_NULL;

    public static Condition get(String p) {
      if (p == null) {
        return null;
      } else {
        try {
          return Condition.valueOf(p.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          return null;
        }
      }
    }
  }

  public static class Assert extends ScriptOp {

    @Override
    public void execute(Script script) throws Exception {
      String key = params.get("key");
      Condition condition = Condition.get(params.required().get("condition"));
      if (condition == null) {
        throw new IOException("Invalid 'condition' in params: " + params);
      }
      String expected = params.get("expected");
      if (condition != Condition.NOT_NULL && condition != Condition.NULL && expected == null) {
        throw new IOException("'expected' param is required when condition is " + condition);
      }
      Object value;
      if (key != null) {
        if (key.contains("/")) {
          value = Utils.getObjectByPath(script.context, true, key);
        } else {
          value = script.context.get(key);
        }
      } else {
        value = params.required().get("value");
      }
      switch (condition) {
        case NULL:
          if (value != null) {
            throw new IOException("expected value should be null but was '" + value + "'");
          }
          break;
        case NOT_NULL:
          if (value == null) {
            throw new IOException("expected value should not be null");
          }
          break;
        case EQUALS:
          if (!expected.equals(String.valueOf(value))) {
            throw new IOException("expected value is '" + expected + "' but actual value is '" + value + "'");
          }
          break;
        case NOT_EQUALS:
          if (expected.equals(String.valueOf(value))) {
            throw new IOException("expected value is '" + expected + "' and actual value is the same while it should be different");
          }
          break;
      }
    }
  }

  public static Script loadResource(SolrResourceLoader loader, CloudSolrClient client, String nodeName, String resource) throws Exception {
    String data;
    try {
      InputStream in = loader.openResource(resource);
      data = IOUtils.toString(in, "UTF-8");
    } catch (IOException e) {
      throw new Exception("cannot open script resource " + resource, e);
    }
    return load(client, nodeName, data);
  }

  /**
   * Parse a DSL string and create a scenario ready to run.
   * @param client connected Solr client
   * @param nodeName my node name, if applicable, or null
   * @param data DSL string with commands and parameters
   * @return configured scenario
   * @throws Exception on syntax errors
   */
  public static Script load(CloudSolrClient client, String nodeName, String data) throws Exception {
    Objects.requireNonNull(client, "Solr client must not be null here");
    Objects.requireNonNull(data, "script data must not be null");
    @SuppressWarnings("resource")
    Script script = new Script(client, nodeName);
    String[] lines = data.split("\\r?\\n");
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      line = line.trim();
      if (line.trim().isEmpty() || line.startsWith("#") || line.startsWith("//")) {
        continue;
      }
      // remove trailing / / comments
      String[] comments = line.split("//");
      String expr = comments[0];
      // split on blank
      String[] parts = expr.split("\\s+");
      if (parts.length > 2) {
        log.warn("Invalid line - wrong number of parts {}, skipping: {}", parts.length, line);
        continue;
      }
      ScriptAction action = ScriptAction.get(parts[0]);
      if (action == null) {
        log.warn("Invalid script action {}, skipping...", parts[0]);
        continue;
      }
      if (action == ScriptAction.LOOP_END) {
        if (!script.context.containsKey("loop")) {
          throw new IOException("LOOP_END without start!");
        }
        script.context.remove("loop");
        continue;
      }
      Class<? extends ScriptOp> opClass = supportedOps.get(action);
      ScriptOp op = opClass.getConstructor().newInstance();
      ModifiableSolrParams params = new ModifiableSolrParams();
      if (parts.length > 1) {
        String paramsString = parts[1];
        if (parts[1].contains("?")) { // url-like with path?params...
          String[] urlParts = parts[1].split("\\?");
          params.set("path", urlParts[0]);
          paramsString = urlParts.length > 1 ? urlParts[1] : "";
        }
        String[] paramsParts = paramsString.split("&");
        for (String paramPair : paramsParts) {
          String[] paramKV = paramPair.split("=");
          String k = URLDecoder.decode(paramKV[0], "UTF-8");
          String v = paramKV.length > 1 ? URLDecoder.decode(paramKV[1], "UTF-8") : null;
          params.add(k, v);
        }
      }
      op.init(params);
      // loop handling
      if (action == ScriptAction.LOOP_START) {
        if (script.context.containsKey("loop")) {
          throw new IOException("only one loop level is allowed");
        }
        script.context.put("loop", op);
        script.ops.add(op);
        continue;
      }
      LoopOp currentLoop = (LoopOp) script.context.get("loop");
      if (currentLoop != null) {
        currentLoop.ops.add(op);
      } else {
        script.ops.add(op);
      }
    }
    if (script.context.containsKey("loop")) {
      throw new IOException("Unterminated loop statement");
    }
    return script;
  }

  public void run() throws Exception {
    ExecutorService executor = ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("Script"));
    try {
      run(executor);
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Run the script.
   */
  public void run(ExecutorService executor) throws Exception {
    for (int i = 0; i < ops.size(); i++) {
      if (abortScript) {
        log.info("-- abortScript requested, aborting after {} ops.", i);
        return;
      }
      ScriptOp op = ops.get(i);
      if (log.isInfoEnabled()) {
        log.info("{}.\t{}\t{}", i + 1, op.getClass().getSimpleName(), op.initParams); // logOk
      }
      // substitute parameters based on the current context
      ClusterStateProvider clusterStateProvider = client.getClusterStateProvider();
      ArrayList<String> liveNodes = new ArrayList<>(clusterStateProvider.getLiveNodes());
      context.put(LIVE_NODES_CTX_PROP, liveNodes);
      String randomNode = liveNodes.get(random.nextInt(liveNodes.size()));
      context.put(RANDOM_NODE_CTX_PROP, randomNode);
      ClusterState clusterState = clusterStateProvider.getClusterState();
      context.put(COLLECTIONS_CTX_PROP, new ArrayList<>(clusterState.getCollectionStates().keySet()));
      if (nodeName != null) {
        context.put(NODE_NAME_CTX_PROP, nodeName);
      }
      op.prepareCurrentParams(this);
      if (log.isInfoEnabled()) {
        log.info("\t\t{}\t{}", op.getClass().getSimpleName(), op.params);
      }
      final ErrorHandling currentErrorHandling = getErrorHandling();
      Future<Exception> res = executor.submit(() -> {
        try {
          op.execute(this);
          return null;
        } catch (Exception e) {
          this.abortScript = currentErrorHandling != ErrorHandling.IGNORE ? true : false;
          return e;
        }
      });
      long timeout = Long.parseLong(String.valueOf(context.getOrDefault(TIMEOUT_PROP, DEFAULT_OP_TIMEOUT_MS)));
      try {
        Exception error = res.get(timeout, TimeUnit.MILLISECONDS);
        if (error != null) {
          throw error;
        }
      } catch (TimeoutException e) {
        throw new Exception("Timeout executing op " + op);
      } catch (InterruptedException e) {
        throw new Exception("Interrupted while executing op " + op);
      } catch (Exception e) {
        if (currentErrorHandling != ErrorHandling.IGNORE) {
          throw e;
        } else {
          continue;
        }
      }
    }
  }

  public ErrorHandling getErrorHandling() {
    return ErrorHandling.get(String.valueOf(context.getOrDefault(ERROR_HANDLING_PROP, ErrorHandling.END_SCRIPT)));
  }

  @Override
  public void close() throws IOException {
  }
}

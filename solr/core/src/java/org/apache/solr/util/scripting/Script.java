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
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.PropertiesUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * This class represents a script consisting of a series of operations.
 */
public class Script implements Closeable {
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
  /** Context variable: Last op result. */
  public static final String LAST_RESULT_CTX_PROP = "_last_result_";
  /** Context variable: Current loop iteration or none if outside of loop. */
  public static final String LOOP_ITER_PROP = "_loop_iter_";
  /** Context variable: Timeout for operation to complete, in ms. */
  public static final String TIMEOUT_PROP = "_timeout_";
  /** Context variable: How to handle errors. */
  public static final String ERROR_HANDLING_PROP = "_error_handling_";

  public enum ErrorHandling {
    IGNORE,
    ABORT,
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

  public static final int DEFAULT_OP_TIMEOUT_MS = 120000;

  public final List<ScriptOp> ops = new ArrayList<>();
  public final Map<String, Object> context = new HashMap<>();
  public final CloudSolrClient client;
  public final String nodeName;
  public final SolrCloudManager cloudManager;
  public final TimeSource timeSource = TimeSource.NANO_TIME;
  public final Random random = new Random();
  public final List<OpResult> opResults = new ArrayList<>();
  private ExecutorService executorService;
  private final boolean shouldCloseExecutor;
  public boolean verbose;
  public boolean abortLoop;
  public boolean abortScript;
  public ErrorHandling errorHandling = ErrorHandling.ABORT;

  public Function<String, String> propertyEvalFunc = (key) -> {
    Object o = Utils.getObjectByPath(context, false, key);
    if (o != null) {
      return o.toString();
    } else {
      return null;
    }
  };

  /** Base class for implementation of script DSL ScriptActions. */
  public static abstract class ScriptOp implements MapWriter {
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
      ModifiableSolrParams currentParams = new ModifiableSolrParams();
      initParams.forEach(e -> {
        String newKey = PropertiesUtil.substitute(e.getKey(), script.propertyEvalFunc);
        if (newKey == null) {
          newKey = e.getKey();
        }
        String[] newValues;
        if (e.getValue() != null && e.getValue().length > 0) {
          String[] values = e.getValue();
          newValues = new String[values.length];
          for (int k = 0; k < values.length; k++) {
            String newVal = PropertiesUtil.substitute(values[k], script.propertyEvalFunc);
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
     * @return result of the operation (status, response, etc) for informative purpose
     */
    public abstract Object execute (Script script) throws Exception;

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + "{" + initParams + "}";
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("name", getClass().getSimpleName());
      ew.put("initParams", initParams);
      if (params != null) {
        ew.put("params", params);
      }
    }
  }

  public static class OpResult implements MapWriter {
    public final ScriptOp op;
    public final Object result;

    public OpResult(ScriptOp op, Object result) {
      this.op = op;
      this.result = result;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("op", op);
      ew.put("result", result);
    }
  }


  /**
   * Actions supported by the script.
   */
  public enum ScriptAction {
    /** Start a loop. */
    LOOP,
    /** End a loop of if/else. */
    END,
    /** Start a conditional block. */
    IF,
    /** Start an ELSE conditional block. */
    ELSE,
    /** Execute a SolrRequest. */
    REQUEST,
    /** Wait for a collection to reach the indicated number of shards and replicas. */
    WAIT_COLLECTION,
    /** Wait for a replica (or core) to reach the indicated state. */
    WAIT_REPLICA,
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
    /** Log arbitrary data from context. */
    LOG,
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
    supportedOps.put(ScriptAction.LOOP, LoopOp.class);
    supportedOps.put(ScriptAction.REQUEST, RequestOp.class);
    supportedOps.put(ScriptAction.WAIT, WaitOp.class);
    supportedOps.put(ScriptAction.WAIT_COLLECTION, WaitCollectionOp.class);
    supportedOps.put(ScriptAction.WAIT_REPLICA, WaitReplicaOp.class);
    supportedOps.put(ScriptAction.SET, CtxSetOp.class);
    supportedOps.put(ScriptAction.CLEAR, CtxClearOp.class);
    supportedOps.put(ScriptAction.ASSERT, AssertOp.class);
    supportedOps.put(ScriptAction.TIMEOUT, TimeoutOp.class);
    supportedOps.put(ScriptAction.LOG, LogOp.class);
    supportedOps.put(ScriptAction.IF, IfElseOp.class);
    supportedOps.put(ScriptAction.ELSE, null);
    supportedOps.put(ScriptAction.END, null);
    supportedOps.put(ScriptAction.DUMP, DumpOp.class);
  }

  private Script(CloudSolrClient client, String nodeName) {
    this(client, nodeName, null);
  }

  private Script(CloudSolrClient client, String nodeName, ExecutorService executorService) {
    this.client = client;
    this.nodeName = nodeName;
    this.cloudManager = new SolrClientCloudManager(null, client);
    if (executorService == null) {
      this.executorService = ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("Script"));
      this.shouldCloseExecutor = true;
    } else {
      this.executorService = executorService;
      this.shouldCloseExecutor = false;
    }
    // We make things reproducible in tests by using test seed if any
    String seed = System.getProperty("tests.seed");
    if (seed != null) {
      random.setSeed(seed.hashCode());
    }
  }

  /**
   * Log some information.
   */
  public static class LogOp extends ScriptOp {

    @Override
    public Object execute(Script script) throws Exception {
      String msg = params.get("msg");
      if (msg == null) {
        String[] keys = params.required().getParams("key");
        String format = params.required().get("format");
        int count = StringUtils.countMatches(format, "{}");
        if (count > keys.length) {
          throw new Exception("Too few key names for the number of formal parameters: " + count);
        } else if (count < keys.length) {
          throw new Exception("Too many key names for the number of formal parameters: " + count);
        }
        Object[] vals = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
          vals[i] = Utils.getObjectByPath(script.context, false, keys[i]);
        }
        log.info(format, vals);
      } else {
        log.info(msg);
      }
      return null;
    }
  }

  public static abstract class CompoundOp extends ScriptOp {
    // populated by the DSL parser
    public List<ScriptOp> ops = new ArrayList<>();

    public void addOp(ScriptOp op) {
      ops.add(op);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(this.getClass().getSimpleName() + "{" + params + ", ops=" + ops.size() + "}");
      for (int i = 0; i < ops.size(); i++) {
        sb.append("\n    " + (i + 1) + ". " + ops.get(i));
      }
      return sb.toString();
    }
  }

  /**
   * Loop ScriptAction.
   */
  public static class LoopOp extends CompoundOp {
    int iterations;

    @Override
    public Object execute(Script script) throws Exception {
      iterations = params.required().getInt("iterations", 10);
      if (iterations < 0) {
        throw new Exception("Number of iterations must be non-negative but was " + iterations);
      }
      for (int i = 0; i < iterations; i++) {
        if (script.abortLoop) {
          if (log.isDebugEnabled()) {
            log.debug("        -- abortLoop requested, aborting after {} iterations.", i);
          }
          return Map.of("iterations", i, "state", "aborted");
        }
        script.context.put(LOOP_ITER_PROP, String.valueOf(i));
        int k = i + 1; // can't use a plus in a logging stmt :)
        if (log.isDebugEnabled()) {
          log.debug("   * iter {} :", k);
        }
        for (ScriptOp op : ops) {
          if (log.isInfoEnabled()) {
            log.info("     - {}\t{})", op.getClass().getSimpleName(), op.params);
          }
          script.execOp(op);
          if (script.abortLoop) {
            if (log.isDebugEnabled()) {
              log.debug("        -- abortLoop requested, aborting after {} iterations.", i);
            }
            return Map.of("iterations", i, "state", "aborted");
          }
        }
      }
      return Map.of("iterations", iterations, "state", "finished");
    }
  }

  public static class IfElseOp extends CompoundOp {
    public List<ScriptOp> elseOps = new ArrayList<>();
    public boolean parsingElse = false;

    @Override
    public void addOp(ScriptOp op) {
      if (parsingElse) {
        elseOps.add(op);
      } else {
        ops.add(op);
      }
    }

    @Override
    public Object execute(Script script) throws Exception {
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
        value = Utils.getObjectByPath(script.context, true, key);
      } else {
        value = params.required().get("value");
      }
      boolean eval = Condition.eval(condition, value, expected);
      List<ScriptOp> selectedOps = eval ? ops : elseOps;
      for (ScriptOp op : selectedOps) {
        if (log.isInfoEnabled()) {
          log.info("     - {}\t{})", op.getClass().getSimpleName(), op.params);
        }
        script.execOp(op);
      }
      return Map.of("condition_eval", eval);
    }
  }

  /**
   * Set a context property.
   */
  public static class CtxSetOp extends ScriptOp {
    @Override
    public Object execute(Script script) throws Exception {
      String key = params.required().get("key");
      String value = params.required().get("value");
      Object previous;
      if (value != null) {
        previous = script.context.put(key, value);
      } else {
        previous = script.context.remove(key);
      }
      Map<String, Object> res = new HashMap<>();
      res.put("previous", previous);
      res.put("new", value);
      return res;
    }
  }

  /**
   * Remove a context property.
   */
  public static class CtxClearOp extends ScriptOp {
    @Override
    public Object execute(Script script) throws Exception {
      String key = params.required().get("key");
      return script.context.remove(key);
    }
  }

  /**
   * Dump the script, execution trace and the current context to log output.
   */
  public static class DumpOp extends ScriptOp {

    private void logLine(String format, Object... params) {
      if (log.isInfoEnabled()) {
        log.info(format, params);
      }
    }

    @Override
    public Object execute(Script script) throws Exception {
      logLine("========= START script dump ==========");
      logLine("------    Script Operations    --------");
      for (int i = 0; i < script.ops.size(); i++) {
        logLine("{}.\t{}", i + 1, script.ops.get(i));
      }
      logLine("------    Script Execution     --------");
      for (int i = 0; i < script.opResults.size(); i++) {
        OpResult res = script.opResults.get(i);
        dumpResult(res, i + 1, 0);
      }
      logLine("------  Final Script Context   --------");
      TreeMap<String, Object> map = new TreeMap<>(script.context);
      map.forEach((key, value) -> {
        if (value instanceof Collection) {
          @SuppressWarnings("unchecked")
          Collection<Object> collection = (Collection<Object>) value;
          logLine("{} size={}", key, collection.size());
          int i = 1;
          for (Object o : collection) {
            logLine("\t{}.\t{}", i, o);
            i++;
          }
        } else if (value instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> m = (Map<String, Object>) value;
          logLine("{} size={}", key, m.size());
          m.forEach((k, v) -> logLine("\t{}\t{}", k, v));
        } else {
          if (value instanceof OpResult) {
            dumpResult((OpResult) value, 0, 0);
          } else {
            logLine("{}\t{}", key, value);
          }
        }
      });
      logLine("=========  END script dump  ==========");
      return null;
    }

    private void dumpResult(OpResult res, int index, int indent) {
      StringBuilder indentSb = new StringBuilder();
      for (int i = 0; i < indent; i++) {
        indentSb.append('\t');
      }
      String indentStr = indentSb.toString();
      logLine("{}{}.\t{}", indentStr, index, res.op);
      if (res.result == null) {
        return;
      }
      if (res.result instanceof Collection) {
        @SuppressWarnings("unchecked")
        Collection<Object> col = (Collection<Object>) res.result;
        logLine("{}\tRes: size={}", indentStr, col.size());
        int k = 1;
        for (Object o : col) {
          if (o instanceof OpResult) { // LOOP op results
            dumpResult((OpResult) o, k, indent + 1);
          } else {
            logLine("{}\t-\t{}. {}", indentStr, k, o);
          }
          k++;
        }
      } else if (res.result instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) res.result;
        logLine("{}\tRes: size={}", indentStr, map.size());
        map.forEach((k, v) -> logLine("{}\t-\t{}\t{}", indentStr, k, v));
      } else if (res.result instanceof MapWriter) {
        @SuppressWarnings("unchecked")
        MapWriter mw = (MapWriter) res.result;
        logLine("{}\tRes: size={}", indentStr, mw._size());
        logLine("{}\t{}", indentStr, mw.jsonStr());
      } else {
        logLine("{}\tRes: {}", indentStr, res.result);
      }
    }
  }

  /**
   * Execute a SolrRequest.
   */
  public static class RequestOp extends ScriptOp {
    @Override
    public Object execute(Script script) throws Exception {
      String path = params.get("path", "/");
      SolrRequest.METHOD m = SolrRequest.METHOD.valueOf(params.get("httpMethod", "GET"));
      params.remove("httpMethod");
      String streamBody = params.get("stream.body");
      params.remove("stream.body");
      GenericSolrRequest req = new GenericSolrRequest(m, path, params);
      if (streamBody != null) {
        req.setContentWriter(new RequestWriter.StringPayloadContentWriter(streamBody, "application/json"));
      }
      return script.client.request(req);
    }
  }

  /**
   * Set (or reset) the timeout for operations to execute.
   */
  public static class TimeoutOp extends ScriptOp {
    @Override
    public Object execute(Script script) throws Exception {
      String value = params.get("value", "" + DEFAULT_OP_TIMEOUT_MS);
      long newTimeout = DEFAULT_OP_TIMEOUT_MS;
      if (!value.equals("default")) {
        newTimeout = Long.parseLong(value);
      }
      script.context.put(TIMEOUT_PROP, newTimeout);
      return null;
    }
  }

  /**
   * Sit idle for a while, 10s by default.
   */
  public static class WaitOp extends ScriptOp {
    @Override
    public Object execute(Script script) throws Exception {
      int timeMs = params.getInt("time", 10000);
      script.timeSource.sleep(timeMs);
      return null;
    }
  }

  /**
   * Wait for a specific collection shape.
   */
  public static class WaitCollectionOp extends ScriptOp {
    @Override
    public Object execute(Script script) throws Exception {
      String collection = params.required().get("collection");
      int shards = Integer.parseInt(params.required().get("shards"));
      int replicas = Integer.parseInt(params.required().get("replicas"));
      boolean withInactive = params.getBool("withInactive", false);
      boolean requireLeaders = params.getBool("requireLeaders", true);
      int waitSec = params.required().getInt("wait", CloudUtil.DEFAULT_TIMEOUT);
      try {
        CloudUtil.waitForState(script.cloudManager, collection, waitSec, TimeUnit.SECONDS,
            CloudUtil.clusterShape(shards, replicas, withInactive, requireLeaders));
      } catch (Exception e) {
        DocCollection coll = script.cloudManager.getClusterStateProvider().getCollection(collection);
        throw new Exception("last collection state: " + coll, e);
      }
      DocCollection coll = script.cloudManager.getClusterStateProvider().getCollection(collection);
      return Utils.fromJSONString(Utils.toJSONString(coll));
    }
  }

  /**
   * Wait for specific core to reach a state.
   */
  public static class WaitReplicaOp extends ScriptOp {
    @Override
    public Object execute(Script script) throws Exception {
      String collection = params.required().get("collection");
      String coreName = params.get("core");
      String replicaName = params.get("replica");
      if (coreName == null && replicaName == null) {
        throw new Exception("Either 'core' or 'replica' must be specified.");
      } else if (coreName != null && replicaName != null) {
        throw new Exception("Only one of 'core' or 'replica' must be specified.");
      }
      int waitSec = params.required().getInt("wait", CloudUtil.DEFAULT_TIMEOUT);
      String stateStr = params.get("state", Replica.State.ACTIVE.toString());
      Replica.State expectedState = Replica.State.getState(stateStr);

      TimeOut timeout = new TimeOut(waitSec, TimeUnit.SECONDS, script.timeSource);
      DocCollection coll = null;
      Replica replica = null;
      while (!timeout.hasTimedOut()) {
        ClusterState clusterState = script.cloudManager.getClusterStateProvider().getClusterState();
        coll = clusterState.getCollectionOrNull(collection);
        if (coll == null) { // does not yet exist?
          timeout.sleep(250);
          continue;
        }
        for (Replica r : coll.getReplicas()) {
          if (coreName != null && coreName.equals(r.getCoreName())) {
            replica = r;
            break;
          } else if (replicaName != null && replicaName.equals(r.getName())) {
            replica = r;
            break;
          }
        }
        if (replica == null) {
          timeout.sleep(250);
          continue;
        }
        if (replica.getState() != expectedState) {
          timeout.sleep(250);
          continue;
        } else {
          break;
        }
      }
      if (timeout.hasTimedOut()) {
        String msg = "Timed out waiting for replica: collection=" + collection + ", ";
        if (coreName != null) {
          msg += "core=" + coreName;
        } else {
          msg += "replica=" + replicaName;
        }
        if (coll == null) {
          msg += ". Collection '" + collection + " not found.";
        } else if (replica == null) {
          msg += ". Replica not found, last collection state: " + coll;
        } else {
          msg += ". Replica did not reach desired state, last state: " + replica;
        }
        throw new Exception(msg);
      }
      return replica;
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

    public static boolean eval(Condition condition, Object value, String expected) {
      switch (condition) {
        case NULL:
          if (value != null) {
            return false;
          }
          break;
        case NOT_NULL:
          if (value == null) {
            return false;
          }
          break;
        case EQUALS:
          if (!expected.equals(String.valueOf(value))) {
            return false;
          }
          break;
        case NOT_EQUALS:
          if (expected.equals(String.valueOf(value))) {
            return false;
          }
          break;
      }
      return true;
    }
  }

  public static class AssertOp extends ScriptOp {

    @Override
    public Object execute(Script script) throws Exception {
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
        value = Utils.getObjectByPath(script.context, true, key);
      } else {
        value = params.required().get("value");
      }
      if (!Condition.eval(condition, value, expected)) {
        throw new Exception("Assertion failed: expected=" + expected +
            ", condition=" + condition +
            ", value=" + value);
      }
      return Boolean.TRUE;
    }
  }

  public static Script parseResource(SolrResourceLoader loader, CloudSolrClient client, String nodeName, String resource) throws Exception {
    String data;
    try {
      InputStream in = loader.openResource(resource);
      data = IOUtils.toString(in, "UTF-8");
    } catch (IOException e) {
      throw new Exception("cannot open script resource " + resource, e);
    }
    return parse(client, nodeName, data);
  }

  private static final String PROP_PREFIX = "script.";

  /**
   * Parse a DSL string and create a script ready to run.
   * @param client connected Solr client
   * @param nodeName my node name, if applicable, or null
   * @param data DSL string with commands and parameters
   * @return configured script
   * @throws Exception on syntax errors
   */
  public static Script parse(CloudSolrClient client, String nodeName, String data) throws Exception {
    Objects.requireNonNull(client, "Solr client must not be null here");
    Objects.requireNonNull(data, "script data must not be null");
    @SuppressWarnings("resource")
    Script script = new Script(client, nodeName);
    // process system properties and put them in context
    System.getProperties().forEach((name, value) -> {
      String key = name.toString();
      if (!key.startsWith(PROP_PREFIX)) {
        return;
      }
      script.context.put(key.substring(PROP_PREFIX.length()), value);
    });
    Stack<CompoundOp> compoundOps = new Stack<>();
    Stack<Integer> compoundStarts = new Stack<>();
    String[] lines = data.split("\\r?\\n");
    for (int i = 0; i < lines.length; i++) {
      int lineNum = i + 1;
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
        throw new Exception("Syntax error on line " + lineNum + ": invalid line - wrong number of parts " + parts.length + ", expected at most 2.");
      }
      ScriptAction action = ScriptAction.get(parts[0]);
      if (action == null) {
        throw new Exception("Syntax error on line " + lineNum + ": invalid script action '" + parts[0] + "'.");
      }
      if (action == ScriptAction.END) {
        if (compoundOps.isEmpty()) {
          throw new Exception("Syntax error on line " + lineNum + ": compound block END without start.");
        }
        compoundOps.pop();
        compoundStarts.pop();
        continue;
      } else if (action == ScriptAction.ELSE) {
        if (!(compoundOps.peek() instanceof IfElseOp)) {
          throw new Exception("Syntax error on line " + lineNum + ": ELSE without IF start.");
        }
        ((IfElseOp) compoundOps.peek()).parsingElse = true;
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
      if (action == ScriptAction.LOOP || action == ScriptAction.IF) {
        compoundOps.add((CompoundOp) op);
        compoundStarts.add(lineNum);
        script.ops.add(op);
        continue;
      }
      CompoundOp currentCompound = compoundOps.isEmpty() ? null : compoundOps.peek();
      if (currentCompound != null) {
        currentCompound.addOp(op);
      } else {
        script.ops.add(op);
      }
    }
    if (!compoundOps.isEmpty()) {
      throw new Exception("Syntax error on EOF: unterminated statement " +
          compoundOps.peek() + ". Started on line " + compoundStarts.peek());
    }
    return script;
  }

  /**
   * Run the script. This is a one-shot operation
   */
  public void run() throws Exception {
    if (executorService == null) {
      throw new Exception("This script instance cannot be reused.");
    }
    try {
      for (int i = 0; i < ops.size(); i++) {
        if (abortScript) {
          if (log.isInfoEnabled()) {
            log.info("-- abortScript requested, aborting after {} ops.", i);
          }
          return;
        }
        ScriptOp op = ops.get(i);
        int k = i + 1; // can't use a plus in a logging stmt
        if (log.isInfoEnabled()) {
          log.info("{}.\t{}\t{}", k, op.getClass().getSimpleName(), op.initParams);
        }
        execOp(op);
      }
    } catch (Exception e) {
      throw e;
    } finally {
      close();
    }
  }

  private void execOp(ScriptOp op) throws Exception {
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
    OpResult opResult = null;
    if (op instanceof LoopOp) {
      // execute directly - the loop will submit its ops to the same executor
      try {
        Object loopRes = op.execute(this);
        if (loopRes instanceof Exception) {
          throw (Exception) loopRes;
        } else {
          opResult = new OpResult(op, loopRes);
          opResults.add(opResult);
        }
      } catch (InterruptedException e) {
        throw new Exception("Interrupted while executing op " + op);
      } catch (Exception e) {
        if (currentErrorHandling != ErrorHandling.IGNORE) {
          throw new Exception("Error executing op " + op, e);
        } else {
          opResult = new OpResult(op, e);
          opResults.add(opResult);
        }
      }
    } else {
      Future<Object> res = executorService.submit(() -> {
        try {
          return op.execute(this);
        } catch (Exception e) {
          this.abortScript = currentErrorHandling != ErrorHandling.IGNORE ? true : false;
          return e;
        }
      });
      long timeout = Long.parseLong(String.valueOf(context.getOrDefault(TIMEOUT_PROP, DEFAULT_OP_TIMEOUT_MS)));
      try {
        Object result = res.get(timeout, TimeUnit.MILLISECONDS);
        if (result instanceof Exception) {
          throw (Exception) result;
        } else {
          opResult = new OpResult(op, result);
          opResults.add(opResult);
        }
      } catch (TimeoutException e) {
        throw new Exception("Timeout executing op " + op);
      } catch (InterruptedException e) {
        throw new Exception("Interrupted while executing op " + op);
      } catch (Exception e) {
        if (currentErrorHandling != ErrorHandling.IGNORE) {
          throw new Exception("Error executing op " + op, e);
        } else {
          opResult = new OpResult(op, e);
          opResults.add(opResult);
        }
      }
    }
    if (log.isInfoEnabled()) {
      log.info("\t\tResult\t{}", opResult.result);
    }
    context.put(LAST_RESULT_CTX_PROP, opResult);
  }

  public ErrorHandling getErrorHandling() {
    return ErrorHandling.get(String.valueOf(context.getOrDefault(ERROR_HANDLING_PROP, ErrorHandling.ABORT)));
  }

  @Override
  public void close() {
    if (executorService != null && shouldCloseExecutor) {
      executorService.shutdownNow();
      try {
        executorService.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // basically ignore, restore interrupted status
        Thread.currentThread().interrupt();
      } finally {
        executorService = null;
      }
    }
  }
}

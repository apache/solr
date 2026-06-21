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
package org.apache.solr.logging;

import org.apache.solr.common.StringUtils;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;

/**
 * Set's per thread context info for logging. Nested calls will use the top level parent for all context. The first
 * caller always owns the context until it calls {@link #clear()}. Always call {@link #clear()} in a finally block.
 */
public class MDCLoggingContext {
  public static final String TRACE_ID = "trace_id";
  // When a thread sets context and finds that the context is already set, we should noop and ignore the finally clear
//
  private static final ThreadLocal<Map<String, String>> contextMap = null; //= new InheritableThreadLocal<>() {
//    @Override protected Map<String,String> childValue(final Map<String,String> parentValue) {
//      return parentValue != null ? Collections.unmodifiableMap(new HashMap<>(parentValue)) : null;
//    }
//  };

  public static void setTracerId(String traceId) {
    if (!StringUtils.isEmpty(traceId)) {
      MDC.put(TRACE_ID, traceId);
    } else {
      MDC.remove(TRACE_ID);
    }
  }
  
  public static void setCoreName(String core) {
    if (core != null) {
      MDC.put(CORE_NAME_PROP, core);
    } else {
      MDC.remove(CORE_NAME_PROP);
    }

    if (core != null && contextMap != null) {
      Map<String,String> map = contextMap.get();
      map = map == null ? new HashMap<>(4) : new HashMap<>(map);
      map.put(CORE_NAME_PROP, core);
      contextMap.set(Collections.unmodifiableMap(map));
    } else if (contextMap != null){
      Map<String,String> map = contextMap.get();
      if (map == null) {
        return;
      }
      map = new HashMap<>(map);
      map.remove(CORE_NAME_PROP);
      contextMap.set(Collections.unmodifiableMap(map));
    }
  }
  
  // we allow the host to be set like this because it is the same for any thread
  // in the thread pool - we can't do this with the per core properties!
  public static void setNode(String node) {
      setNodeName(node);
  }
  
  private static void setNodeName(String node) {
    if (node != null) {
      MDC.put(NODE_NAME_PROP, node);
    } else {
      MDC.remove(NODE_NAME_PROP);
    }

    if (node != null && contextMap != null) {
      Map<String,String> map = contextMap.get();
      map = map == null ? new HashMap<>(4) : new HashMap<>(map);
      map.put(NODE_NAME_PROP, node);
      contextMap.set(Collections.unmodifiableMap(map));
    } else if (contextMap != null) {
      Map<String,String> map = contextMap.get();
      if (map == null) {
        return;
      }
      map = new HashMap<>(map);
      map.remove(NODE_NAME_PROP);
      contextMap.set(Collections.unmodifiableMap(map));
    }
  }

  public static String getNodeName() {
    return MDC.get(NODE_NAME_PROP);
  }

  /**
   * Call this in a
   * finally.
   */
  public static void clear() {
    MDC.remove(CORE_NAME_PROP);
    if (contextMap != null) {
      Map<String,String> map = contextMap.get();
      map = map == null ? new HashMap<>(4) : new HashMap<>(map);
      map.remove(CORE_NAME_PROP);

      contextMap.set(Collections.unmodifiableMap(map));
    }
  }
  
  private static void removeAll() {
    MDC.remove(CORE_NAME_PROP);
    MDC.remove(NODE_NAME_PROP);
    MDC.remove(TRACE_ID);
    if (contextMap != null) contextMap.remove();
  }

  /** Resets to a cleared state.  Used in-between requests into Solr. */
  public static void reset() {
    removeAll();
  }
}

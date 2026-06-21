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
package org.apache.solr.common.util;

import org.apache.commons.io.output.StringBuilderWriter;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ObjectReleaseTrackerTestImpl extends ObjectReleaseTracker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static Map<String,StackTraceElement[]> OBJECTS = new NonBlockingHashMap<>();
  private static final boolean DISABLED = Boolean.getBoolean("disableCloseTracker");

  protected final static ThreadLocal<StringBuilder> THREAD_LOCAL_SB = new ThreadLocal<>();

  public static StringBuilder getThreadLocalStringBuilder() {
    StringBuilder sw = THREAD_LOCAL_SB.get();
    if (sw == null) {
      sw = new StringBuilder(1024);
      THREAD_LOCAL_SB.set(sw);
    }
    return sw;
  }

  public boolean track(Object object) {
    if (DISABLED) return true;
    ObjectTrackerException ote = new ObjectTrackerException(object.getClass().getName() + "@" + Integer.toHexString(object.hashCode()));
    OBJECTS.put(object.getClass().getName() + "@" + Integer.toHexString(object.hashCode()), Thread.currentThread().getStackTrace());
    return true;
  }
  
  public boolean release(Object object) {
    if (DISABLED) return true;
    OBJECTS.remove(object.getClass().getName() + "@" + Integer.toHexString(object.hashCode()));
    return true;
  }
  
  public void clear() {
    OBJECTS.clear();
  }

  /**
   * @return null if ok else error message
   */
  public String checkEmpty() {
    return checkEmpty(null);
  }

  /**
   * @return null if ok else error message
   * @param object tmp feature allowing to ignore and close an object
   */
  public String checkEmpty(String object) {
   // if (true) return null; // MRM TODO:
    StringBuilder error = null;
    Map<String,StackTraceElement[]> entries = new HashMap<>(OBJECTS);

    if (entries.size() > 0) {
      List<String> objects = new ArrayList<>(entries.size());
      for (Entry<String,StackTraceElement[]> entry : entries.entrySet()) {
        if (object != null && entry.getKey().getClass().getSimpleName().equals(object)) {
          entries.remove(entry.getKey());
          continue;
        }
        objects.add(entry.getKey());
      }
      if (objects.isEmpty()) {
        return null;
      }
      if (error == null) {
        error = new StringBuilder(512);
      }
      error.append("ObjectTracker found ").append(objects.size()).append(" object(s) that were not released!!! ").append(objects).append("\n");
      StringBuilder finalError = error;
      entries.forEach((key, ote) -> {
        StringBuilder sb = getThreadLocalStringBuilder();
        sb.setLength(0);
        StringBuilderWriter sw = new StringBuilderWriter(sb);
        PrintWriter pw = new PrintWriter(sw);
        printStackTrace(ote, pw, 14);
        String stack = object + "\n" + sw.toString().replaceAll("org\\.apache\\.solr", "o.a.s");
        finalError.append(key).append("\n").append("StackTrace:\n").append(stack).append("\n");
      });
    }
    if (error == null || error.length() == 0) {
      return null;
    }
    return error.toString();
  }

  private static void printStackTrace(StackTraceElement[] trace, PrintWriter p, int stack) {
    // Print our stack trace

    final int min = Math.min(stack, trace.length);
    for (int i = 0; i < min; i++) {
      StackTraceElement traceElement = trace[i];
      p.println("at " + traceElement);
    }
  }

  public static class ObjectTrackerException extends RuntimeException {
    public ObjectTrackerException(String msg) {
      super(msg);
    }

    public ObjectTrackerException(Throwable t) {
      super(t);
    }
  }

}

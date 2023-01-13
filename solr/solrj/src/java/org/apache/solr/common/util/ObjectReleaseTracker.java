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

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectReleaseTracker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final List<String> DEFAULT_STACK_FILTERS =
      Arrays.asList(
          new String[] {
            "org.junit.",
            "junit.framework.",
            "sun.",
            "java.lang.reflect.",
            "com.carrotsearch.randomizedtesting.",
          });

  public static final Map<Object, Exception> OBJECTS = new ConcurrentHashMap<>();

  public static boolean track(Object object) {
    // This is called from within constructors, be careful not to make assumptions about state of
    // object here
    Throwable submitter = ExecutorUtil.submitter.get(); // Could be null
    OBJECTS.put(object, new ObjectTrackerException(object.getClass().getName(), submitter));
    return true;
  }

  public static boolean release(Object object) {
    OBJECTS.remove(object);
    return true;
  }

  public static void clear() {
    OBJECTS.clear();
  }

  /**
   * @return null if ok else error message
   */
  public static String checkEmpty() {
    if (OBJECTS.isEmpty()) {
      return null;
    }

    StringBuilder error = new StringBuilder();
    error
        .append("ObjectTracker found ")
        .append(OBJECTS.size())
        .append(" object(s) that were not released!!! ");

    ArrayList<String> objects = new ArrayList<>(OBJECTS.size());
    for (Object object : OBJECTS.keySet()) {
      Class<?> clazz = object.getClass();
      objects.add(
          clazz.isAnonymousClass() ? clazz.getSuperclass().getSimpleName() : clazz.getSimpleName());
    }
    error.append(objects).append("\n");

    for (Entry<Object, Exception> entry : OBJECTS.entrySet()) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      entry.getValue().printStackTrace(pw);

      error.append(entry.getKey().getClass().getName()).append(":");
      error.append(sw).append("\n");
    }

    return error.toString();
  }

  public static void tryClose() {
    for (Object object : OBJECTS.keySet()) {
      if (object instanceof Closeable) {
        try {
          ((Closeable) object).close();
        } catch (Throwable t) {
          log.error("", t);
        }
      } else if (object instanceof ExecutorService) {
        try {
          ExecutorUtil.shutdownAndAwaitTermination((ExecutorService) object);
        } catch (Throwable t) {
          log.error("", t);
        }
      }
    }
  }

  /**
   * @return null if ok else error message
   */
  public static String clearObjectTrackerAndCheckEmpty() {
    String result = ObjectReleaseTracker.checkEmpty();
    ObjectReleaseTracker.clear();
    return result;
  }

  static class ObjectTrackerException extends RuntimeException {
    ObjectTrackerException(String msg, Throwable submitter) {
      super(msg, submitter);
    }
  }
}

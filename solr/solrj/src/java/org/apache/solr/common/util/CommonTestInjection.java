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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows random faults to be injected in running code during test runs across all solr packages.
 *
 * @lucene.internal
 */
public class CommonTestInjection {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static volatile Map<String, String> additionalSystemProps = null;
  private static volatile Integer delay = null;
  private static final ConcurrentMap<String, Breakpoint> breakpoints = new ConcurrentHashMap<>();

  public static void reset() {
    additionalSystemProps = null;
    delay = null;
  }

  public static void setAdditionalProps(Map<String, String> additionalSystemProps) {
    CommonTestInjection.additionalSystemProps = additionalSystemProps;
  }

  public static Map<String, String> injectAdditionalProps() {
    return additionalSystemProps;
  }

  /**
   * Set test delay (sleep) in unit of millisec
   *
   * @param delay delay in millisec, null to remove such delay
   */
  public static void setDelay(Integer delay) {
    CommonTestInjection.delay = delay;
  }

  /**
   * Inject an artificial delay(sleep) into the code
   *
   * @return true
   */
  public static boolean injectDelay() {
    if (delay != null) {
      try {
        log.info("Start: artificial delay for {}ms", delay);
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        log.info("Finish: artificial delay for {}ms", delay);
      }
    }
    return true;
  }

  /**
   * Injects a breakpoint that pauses the existing code execution, executes the code defined in the
   * breakpoint implementation and then resumes afterward. The breakpoint implementation is looked
   * up by the corresponding key used in {@link BreakpointSetter#setImplementation(String,
   * Breakpoint)}
   *
   * <p>An example usages :
   *
   * <ol>
   *   <li>Inject a precise wait until a race condition is fulfilled before proceeding with original
   *       code execution
   *   <li>Inject a flag to catch exception statement which handles the exception without
   *       re-throwing. This could verify caught exception does get triggered
   * </ol>
   *
   * <p>This should always be a part of an assert statement (ie assert injectBreakpoint(key)) such
   * that it will be skipped for normal code execution
   *
   * @see BreakpointSetter#setImplementation(String, Breakpoint)
   * @param key could simply be the fully qualified class name or more granular like class name +
   *     other id (such as method name). This should only be set by corresponding unit test cases
   *     with CommonTestInjection#setBreakpoint
   * @param args optional arguments list to be passed to the Breakpoint
   */
  public static boolean injectBreakpoint(String key, Object... args) {
    Breakpoint breakpoint = breakpoints.get(key);
    if (breakpoint != null) {
      log.info("Breakpoint with key {} is triggered", key);
      breakpoint.executeAndResume(args);
      log.info("Breakpoint with key {} was executed and normal code execution resumes", key);
    } else {
      log.debug(
          "Breakpoint with key {} is triggered but there's no implementation set. Skipping...",
          key);
    }
    return true;
  }

  public interface Breakpoint {
    /**
     * Code execution should break at where the breakpoint was injected, then it would execute this
     * method and resumes the execution afterward.
     */
    void executeAndResume(Object... args);
  }

  /**
   * Breakpoints should be set via this {@link BreakpointSetter} within the test case and close
   * should be invoked as cleanup. Since this is closeable, it should usually be used in the
   * try-with-resource syntax, such as:
   *
   * <pre>{@code
   * try (BreakpointSetter breakpointSetter = new BreakpointSetter() {
   *     //... test code here that calls breakpointSetter.setImplementation(...)
   * }
   * }</pre>
   */
  public static class BreakpointSetter implements Closeable {
    private Set<String> keys = new HashSet<>();

    /**
     * This is usually set by the test cases.
     *
     * <p>If a breakpoint implementation is set by this method, then code execution would break at
     * the code execution point marked by CommonTestInjection#injectBreakpoint with matching key,
     * executes the provided implementation in the {@link Breakpoint}, then resumes the normal code
     * execution.
     *
     * @see CommonTestInjection#injectBreakpoint(String, Object...)
     * @param key could simply be the fully qualified class name or more granular like class name +
     *     other id (such as method name). This should batch the key used in injectBreakpoint
     * @param implementation The Breakpoint implementation
     */
    public void setImplementation(String key, Breakpoint implementation) {
      if (breakpoints.containsKey(key)) {
        throw new IllegalArgumentException(
            "Cannot redefine Breakpoint implementation with key " + key);
      }
      breakpoints.put(key, implementation);
      keys.add(key);
    }

    @Override
    public void close() throws IOException {
      for (String key : keys) {
        breakpoints.remove(key);
      }
    }
  }
}

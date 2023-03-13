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

import java.lang.invoke.MethodHandles;
import java.util.Map;
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
   * This should ONLY be set from unit test cases.
   *
   * <p>If defined, code execution would break at certain code execution point at the invocation of
   * injectBreakpoint with matching key until the provided method in the {@link Breakpoint}
   * implementation is executed.
   *
   * <p>Setting the breakpoint to null would remove the breakpoint
   *
   * @see CommonTestInjection#injectBreakpoint(String)
   * @param key could simply be the fully qualified class name or more granular like class name +
   *     other id (such as method name). This should batch the key used in {@link
   *     Breakpoint#injectBreakpoint(String)}
   * @param breakpoint The Breakpoint implementation, null to remove the breakpoint
   */
  public static void setBreakpoint(String key, Breakpoint breakpoint) {
    if (breakpoint != null) {
      breakpoints.put(key, breakpoint);
    } else {
      breakpoints.remove(key);
    }
  }

  /**
   * Injects a breakpoint that pauses the existing code execution, executes the code defined in the
   * breakpoint implementation and then resumes afterwards. The breakpoint implementation is looked
   * up by the corresponding key used in {@link CommonTestInjection#setBreakpoint(String,
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
   * This should always be a part of an assert statement (ie assert injectBreakpoint(key)) such that
   * it will be skipped for normal code execution
   *
   * @see CommonTestInjection#setBreakpoint(String, Breakpoint)
   * @param key could simply be the fully qualified class name or more granular like class name +
   *     other id (such as method name). This should only be set by corresponding unit test cases
   *     with CommonTestInjection#setBreakpoint
   */
  public static boolean injectBreakpoint(String key) {
    Breakpoint breakpoint = breakpoints.get(key);
    if (breakpoint != null) {
      breakpoint.executeAndResume();
    }
    return true;
  }

  public interface Breakpoint {
    /**
     * Code execution should break at where the breakpoint was injected, then it would execute this
     * method and resumes the execution afterwards.
     */
    void executeAndResume();
  }
}

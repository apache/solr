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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to collect interesting callers at specific points. These calling points are
 * identified by the calling class' simple name and optionally a method name and the optional
 * maximum count, e.g. <code>MoreLikeThisComponent</code> or <code>ClusteringComponent.finishStage
 * </code>, <code>ClusteringComponent.finishStage:100</code>. A single wildcard name <code>*</code>
 * may be used to mean "any class", which may be useful to e.g. collect all callers using an
 * expression <code>*:-1</code>.
 *
 * <p>Within your caller you should invoke {@link #checkCaller()} to count any matching frames in
 * the current stack, and check if any of the count limits has been reached. Each invocation will
 * increase the call count of the matching expression(s). For one invocation multiple matching
 * expression counts can be affected because all current stack frames are examined against the
 * matching expressions.
 *
 * <p>NOTE: implementation details cause the expression <code>simpleName[:NNN]</code> to be disabled
 * when also any <code>simpleName.anyMethod[:NNN]</code> expression is used for the same class name.
 *
 * <p>NOTE 2: when maximum count is a negative number e.g. <code>simpleName[.someMethod]:-1</code>
 * then only the number of calls to {@link #checkCaller()} for the matching expressions will be
 * reported but {@link #checkCaller()} will never return this expression.
 */
public class CallerMatcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String WILDCARD = "*";

  private final StackWalker stackWalker =
      StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
  // className -> set of method names
  private final Map<String, Set<String>> exactCallers = new HashMap<>();
  private final Map<String, Set<String>> excludeCallers = new HashMap<>();
  // expr -> initial count
  private final Map<String, Integer> maxCounts = new ConcurrentHashMap<>();
  // expr -> current count
  private final Map<String, AtomicInteger> callCounts = new ConcurrentHashMap<>();
  private Set<String> trippedBy = ConcurrentHashMap.newKeySet();

  /**
   * Create an instance that reacts to the specified caller expressions.
   *
   * @param callerExprs list of expressions in the format of <code>
   *     ( simpleClassName[.methodName] | * )[:NNN]</code>. If the list is empty or null then the
   *     first call to {@link #checkCaller()} from any caller will match.
   */
  public CallerMatcher(Collection<String> callerExprs, Collection<String> excludeExprs) {
    for (String callerExpr : callerExprs) {
      String[] exprCount = callerExpr.split(":");
      if (exprCount.length > 2) {
        throw new RuntimeException("Invalid count in callerExpr: " + callerExpr);
      }
      String[] clazzMethod = exprCount[0].split("\\.");
      if (clazzMethod.length > 2) {
        throw new RuntimeException("Invalid method in callerExpr: " + callerExpr);
      }
      Set<String> methods = exactCallers.computeIfAbsent(clazzMethod[0], c -> new HashSet<>());
      if (clazzMethod.length > 1) {
        methods.add(clazzMethod[1]);
      }
      if (exprCount.length > 1) {
        try {
          int count = Integer.parseInt(exprCount[1]);
          maxCounts.put(exprCount[0], count);
          callCounts.put(exprCount[0], new AtomicInteger(0));
        } catch (NumberFormatException e) {
          throw new RuntimeException("Invalid count in callerExpr: " + callerExpr, e);
        }
      }
    }
    for (String excludeExpr : excludeExprs) {
      String[] clazzMethod = excludeExpr.split("\\.");
      if (clazzMethod.length > 2) {
        throw new RuntimeException("Invalid method in excludeExpr: " + excludeExpr);
      }
      Set<String> methods = excludeCallers.computeIfAbsent(clazzMethod[0], c -> new HashSet<>());
      if (clazzMethod.length > 1) {
        methods.add(clazzMethod[1]);
      }
    }
  }

  /**
   * Returns the set of caller expressions that were tripped (reached their count limit). This
   * method can be called after {@link #checkCaller()} returns a matching expression to obtain all
   * expressions that exceeded their count limits.
   */
  public Set<String> getTrippedBy() {
    return Collections.unmodifiableSet(trippedBy);
  }

  /** Returns a map of matched caller expressions to their current call counts. */
  public Map<String, Integer> getCallCounts() {
    return callCounts.entrySet().stream()
        .collect(
            Collectors.toMap(
                e ->
                    e.getKey()
                        + (maxCounts.containsKey(e.getKey())
                            ? ":" + maxCounts.get(e.getKey())
                            : ""),
                e -> e.getValue().get()));
  }

  /**
   * Returns the matching caller expression when its count limit was reached, or empty if no caller
   * or no count limit was reached. Each invocation increases the call count of the matching caller,
   * if any. It's up to the caller to decide whether to continue processing after this count limit
   * is reached. The matching expression returned by this call will be also present in {@link
   * #getTrippedBy()}.
   */
  public Optional<String> checkCaller() {
    return stackWalker.walk(
        s ->
            s.filter(
                    frame -> {
                      // handle exclusions first
                      if (isExcluded(frame)) {
                        return false;
                      }

                      String className = frame.getDeclaringClass().getSimpleName();
                      String method = frame.getMethodName();
                      // now handle the matching expressions

                      if (processAnyMatching(className, method)) {
                        return true;
                      }

                      Set<String> methods = exactCallers.get(className);
                      boolean wildcardMatch = false;
                      if (methods == null) {
                        // check for wildcard
                        methods = exactCallers.get(WILDCARD);
                        if (methods == null) {
                          // no class and no methods specified for this class, so skip
                          return false;
                        } else {
                          wildcardMatch = true;
                        }
                      }
                      // MATCH. Class name was specified, possibly with methods.
                      // If methods is empty then all methods match, otherwise only the
                      // specified methods match.
                      if (methods.isEmpty() || methods.contains(method)) {
                        String expr = wildcardMatch ? WILDCARD : className;
                        if (methods.contains(method)) {
                          expr = expr + "." + method;
                        } else {
                          // even though we don't match/enforce at the method level, still
                          // record the method counts to give better insight into the callers
                          callCounts
                              .computeIfAbsent(className + "." + method, k -> new AtomicInteger(0))
                              .incrementAndGet();
                        }
                        int currentCount =
                            callCounts
                                .computeIfAbsent(expr, k -> new AtomicInteger(0))
                                .incrementAndGet();
                        return processMatchWithLimit(expr, currentCount);
                      } else {
                        return false;
                      }
                    })
                .map(
                    frame ->
                        (frame.getDeclaringClass().getSimpleName().isBlank()
                                ? frame.getClassName()
                                : frame.getDeclaringClass().getSimpleName())
                            + "."
                            + frame.getMethodName())
                .findFirst());
  }

  private boolean isExcluded(StackWalker.StackFrame frame) {
    Class<?> declaring = frame.getDeclaringClass();
    String method = frame.getMethodName();
    // always skip myself
    if (declaring == this.getClass()) {
      return true;
    }
    // skip exclusions, if any
    Set<String> excludeMethods = excludeCallers.get(declaring.getSimpleName());
    if (excludeMethods != null) {
      // skip any method
      if (excludeMethods.isEmpty()) {
        return true;
      } else {
        // or only the matching method
        return excludeMethods.contains(method);
      }
    }
    return false;
  }

  private boolean processAnyMatching(String className, String method) {
    if (exactCallers.isEmpty()) {
      // any caller is an offending caller
      String expr = className + "." + method;
      if (log.isInfoEnabled()) {
        log.info("++++ Tripped by any first caller: {} ++++", expr);
      }
      trippedBy.add(expr);
      callCounts.computeIfAbsent(expr, k -> new AtomicInteger()).incrementAndGet();
      return true;
    } else {
      return false;
    }
  }

  private boolean processMatchWithLimit(String expr, int currentCount) {
    // check if we have a max count for this expression
    if (maxCounts.containsKey(expr)) {
      int maxCount = maxCounts.get(expr);
      // if max count is negative then just report the call count
      if (maxCount < 0) {
        maxCount = Integer.MAX_VALUE;
      }
      if (currentCount > maxCount) {
        if (log.isInfoEnabled()) {
          log.info(
              "++++ Tripped by caller: {}, current count: {}, max: {} ++++",
              expr,
              currentCount,
              maxCounts.get(expr));
        }
        trippedBy.add(expr + ":" + maxCounts.get(expr));
        return true;
      } else {
        return false; // max count not reached, not tripped yet
      }
    } else {
      trippedBy.add(expr);
      if (log.isInfoEnabled()) {
        log.info("++++ Tripped by caller: {} ++++", expr);
      }
      return true; // no max count, so tripped on first call
    }
  }
}

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
package org.apache.solr.search;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to simulate query timeouts at specific points in various components that call {@link
 * QueryLimits#shouldExit()}. These calling points are identified by the calling class' simple name
 * and optionally a method name and the optional maximum count, e.g. <code>MoreLikeThisComponent
 * </code> or <code>
 * ClusteringComponent.finishStage</code>, <code>ClusteringComponent.finishStage:100</code>.
 *
 * <p>NOTE: implementation details cause the expression <code>simpleName</code> to be disabled when
 * also any <code>simpleName.anyMethod[:NNN]</code> expression is used for the same class name.
 *
 * <p>NOTE 2: when maximum count is a negative number e.g. <code>simpleName.someMethod:-1</code>
 * then only the number of calls to {@link QueryLimits#shouldExit()} for that expression will be
 * reported but no limit will be enforced.
 */
public class CallerSpecificQueryLimit implements QueryLimit {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final StackWalker stackWalker =
      StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
  // className -> set of method names
  private final Map<String, Set<String>> interestingCallers = new HashMap<>();
  // expr -> initial count
  private final Map<String, Integer> maxCounts = new HashMap<>();
  // expr -> current count
  private final Map<String, AtomicInteger> callCounts = new HashMap<>();
  private Set<String> trippedBy = new LinkedHashSet<>();

  /**
   * Signal a timeout in places that match the calling classes (and methods).
   *
   * @param callerExprs list of expressions in the format of <code>
   *     simpleClassName[.methodName][:NNN]</code>. If the list is empty or null then the first call
   *     to {@link #shouldExit()} from any caller will match.
   */
  public CallerSpecificQueryLimit(String... callerExprs) {
    this(callerExprs != null ? Arrays.asList(callerExprs) : List.of());
  }

  public CallerSpecificQueryLimit(Collection<String> callerExprs) {
    for (String callerExpr : callerExprs) {
      String[] exprCount = callerExpr.split(":");
      if (exprCount.length > 2) {
        throw new RuntimeException("Invalid count in callerExpr: " + callerExpr);
      }
      String[] clazzMethod = exprCount[0].split("\\.");
      if (clazzMethod.length > 2) {
        throw new RuntimeException("Invalid method in callerExpr: " + callerExpr);
      }
      Set<String> methods =
          interestingCallers.computeIfAbsent(clazzMethod[0], c -> new HashSet<>());
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
  }

  /** Returns the set of caller expressions that were tripped. */
  public Set<String> getTrippedBy() {
    return trippedBy;
  }

  /** Returns a map of tripped caller expressions to their current call counts. */
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

  @Override
  public boolean shouldExit() {
    Optional<String> matchingExpr =
        stackWalker.walk(
            s ->
                s.filter(
                        frame -> {
                          Class<?> declaring = frame.getDeclaringClass();
                          // skip bottom-most frames: myself and QueryLimits
                          if (declaring == this.getClass() || declaring == QueryLimits.class) {
                            return false;
                          }
                          String method = frame.getMethodName();
                          if (interestingCallers.isEmpty()) {
                            // any caller is an offending caller
                            String expr = declaring.getSimpleName() + "." + method;
                            if (log.isInfoEnabled()) {
                              log.info("++++ Limit tripped by any first caller: {} ++++", expr);
                            }
                            trippedBy.add(expr);
                            callCounts
                                .computeIfAbsent(expr, k -> new AtomicInteger())
                                .incrementAndGet();
                            return true;
                          }
                          Set<String> methods = interestingCallers.get(declaring.getSimpleName());
                          if (methods == null) {
                            // no class and no methods specified for this class, so skip
                            return false;
                          }
                          // MATCH. Class name was specified, possibly with methods.
                          // If methods is empty then all methods match, otherwise only the
                          // specified methods match.
                          if (methods.isEmpty() || methods.contains(method)) {
                            String expr = declaring.getSimpleName();
                            if (methods.contains(method)) {
                              expr = expr + "." + method;
                            } else {
                              // even though we don't match/enforce at the method level, still
                              // record the method counts to give better insight into the callers
                              callCounts
                                  .computeIfAbsent(
                                      declaring.getSimpleName() + "." + method,
                                      k -> new AtomicInteger(0))
                                  .incrementAndGet();
                            }
                            int currentCount =
                                callCounts
                                    .computeIfAbsent(expr, k -> new AtomicInteger(0))
                                    .incrementAndGet();
                            // check if we have a max count for this expression
                            if (maxCounts.containsKey(expr)) {
                              int maxCount = maxCounts.getOrDefault(expr, 0);
                              // if max count is negative then just report the call count
                              if (maxCount < 0) {
                                maxCount = Integer.MAX_VALUE;
                              }
                              if (currentCount > maxCount) {
                                if (log.isInfoEnabled()) {
                                  log.info(
                                      "++++ Limit tripped by caller: {}, current count: {}, max: {} ++++",
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
                                log.info("++++ Limit tripped by caller: {} ++++", expr);
                              }
                              return true; // no max count, so tripped on first call
                            }
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
    return matchingExpr.isPresent();
  }

  @Override
  public Object currentValue() {
    return "This class just for testing, not a real limit";
  }
}

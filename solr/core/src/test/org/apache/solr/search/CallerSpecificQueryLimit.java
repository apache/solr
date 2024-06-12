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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.index.QueryTimeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to simulate query timeouts at specific points in various components that call {@link
 * QueryLimits#shouldExit()}. These calling points are identified by the calling class' simple name
 * and optionally a method name, e.g. <code>MoreLikeThisComponent</code> or <code>
 * ClusteringComponent.finishStage</code>.
 */
public class CallerSpecificQueryLimit implements QueryTimeout {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final StackWalker stackWalker =
      StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
  final Map<String, Set<String>> interestingCallers = new HashMap<>();
  public String trippedBy;

  /**
   * Signal a timeout in places that match the calling classes (and methods).
   *
   * @param callerExprs list of expressions in the format of <code>simpleClassName[.methodName]
   *     </code>. If the list is empty or null then the first call to {@link #shouldExit()} from any
   *     caller will match.
   */
  public CallerSpecificQueryLimit(String... callerExprs) {
    if (callerExprs != null && callerExprs.length > 0) {
      for (String callerExpr : callerExprs) {
        String[] clazzMethod = callerExpr.split("\\.");
        if (clazzMethod.length > 2) {
          throw new RuntimeException("Invalid callerExpr: " + callerExpr);
        }
        Set<String> methods =
            interestingCallers.computeIfAbsent(clazzMethod[0], c -> new HashSet<>());
        if (clazzMethod.length > 1) {
          methods.add(clazzMethod[1]);
        }
      }
    }
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
                            // any caller is an interesting caller
                            return true;
                          }
                          Set<String> methods = interestingCallers.get(declaring.getSimpleName());
                          if (methods == null) {
                            return false;
                          }
                          return methods.isEmpty() || methods.contains(method);
                        })
                    .map(
                        frame ->
                            (frame.getDeclaringClass().getSimpleName().isBlank()
                                    ? frame.getClassName()
                                    : frame.getDeclaringClass().getSimpleName())
                                + "."
                                + frame.getMethodName())
                    .findFirst());
    if (matchingExpr.isPresent()) {
      if (log.isInfoEnabled()) {
        log.info("++++ Limit tripped by caller: {} ++++", matchingExpr.get());
      }
      trippedBy = matchingExpr.get();
    }
    return matchingExpr.isPresent();
  }
}

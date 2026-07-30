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

import java.util.Collection;
import java.util.Set;
import org.apache.solr.util.CallerMatcher;

/**
 * Helper class to simulate query timeouts at specific points in various components that call {@link
 * QueryLimits#shouldExit()}. This class uses {@link CallerMatcher} to collect the matching callers
 * information and enforce the count limits.
 */
public class CallerSpecificQueryLimit implements QueryLimit {
  private final CallerMatcher callerMatcher;

  /**
   * Signal a timeout in places that match the calling classes (and methods).
   *
   * @param callerExprs list of expressions in the format of <code>
   *     ( simpleClassName[.methodName] | * )[:NNN]</code>. If the list is empty or null then the
   *     first call to {@link #shouldExit()} from any caller will match.
   */
  public CallerSpecificQueryLimit(Collection<String> callerExprs) {
    // exclude myself and QueryLimits
    callerMatcher =
        new CallerMatcher(
            callerExprs,
            Set.of(
                CallerSpecificQueryLimit.class.getSimpleName(), QueryLimits.class.getSimpleName()));
  }

  public CallerMatcher getCallerMatcher() {
    return callerMatcher;
  }

  @Override
  public boolean shouldExit() {
    return callerMatcher.checkCaller().isPresent();
  }

  @Override
  public Object currentValue() {
    return "This class just for testing, not a real limit";
  }
}

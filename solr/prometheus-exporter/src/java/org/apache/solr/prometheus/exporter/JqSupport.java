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

package org.apache.solr.prometheus.exporter;

import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.exception.JsonQueryException;

/**
 * Single shared place for the jq language {@link Version} used to both compile queries and populate
 * the {@link Scope} they run against. These two must stay in sync: a query compiled against one
 * version referencing builtins loaded for a different version fails at evaluation time, not compile
 * time.
 */
public final class JqSupport {

  private JqSupport() {}

  private static final Version VERSION = Version.LATEST;

  /**
   * Root scope with jq's built-in functions (select, to_entries, startswith, etc.) registered once.
   * Safe to pass directly to {@link JsonQuery#apply}, including concurrently: {@code apply}
   * isolates each evaluation in its own child scope internally, and this scope's function registry
   * is populated once here and never mutated afterward.
   */
  public static final Scope ROOT_SCOPE = Scope.newEmptyScope();

  static {
    BuiltinFunctionLoader.getInstance().loadFunctions(VERSION, ROOT_SCOPE);
  }

  public static JsonQuery compile(String query) throws JsonQueryException {
    return JsonQuery.compile(query, VERSION);
  }
}

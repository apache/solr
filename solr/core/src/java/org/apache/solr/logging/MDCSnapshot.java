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

import java.io.Closeable;
import java.util.Map;
import org.slf4j.MDC;

/**
 * Takes a 'snapshot' of the current MDC context map which is restored on (auto) close. This can be
 * used to ensure that no MDC values set (or overridden) inside of call stack will "leak" beyond
 * that call stack.
 *
 * <pre>
 * try (var mdcSnapshot = MDCSnapshot.create()) {
 *   assert null != mdcSnapshot; // prevent compiler warning
 *
 *   // ... arbitrary calls to MDC methods
 * }
 * </pre>
 *
 * @see org.slf4j.MDC#putCloseable
 */
public final class MDCSnapshot implements Closeable {

  public static MDCSnapshot create() {
    return new MDCSnapshot();
  }

  private final Map<String, String> snapshot;

  private MDCSnapshot() {
    this.snapshot = MDC.getCopyOfContextMap();
  }

  @Override
  public void close() {
    MDC.clear();
    if (snapshot != null && !snapshot.isEmpty()) { // common case optimization
      MDC.setContextMap(snapshot);
    }
  }
}

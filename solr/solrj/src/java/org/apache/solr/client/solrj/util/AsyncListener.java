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

package org.apache.solr.client.solrj.util;

/**
 * Listener for async requests
 *
 * @param <T> The result type returned by the {@code onSuccess} method
 * @deprecated Use the async variants that return CompletableFuture.
 */
@Deprecated
public interface AsyncListener<T> {
  /** Callback method invoked before processing the request */
  default void onStart() {}

  /** Callback method invoked when the request completes successfully */
  void onSuccess(T t);

  /** Callback method invoked when the request completes in failure */
  void onFailure(Throwable throwable);
}

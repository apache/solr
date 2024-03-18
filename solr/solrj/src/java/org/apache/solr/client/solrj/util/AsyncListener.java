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
 */
public interface AsyncListener<T> {
  /**
   * Setup method invoked in the original thread before processing the request on a different
   * thread.
   */
  default void onStart() {}

  /**
   * Callback method invoked in the async thread when the request completes successfully.
   *
   * @param t the key object for the async operation (i. e. the SolrRequest)
   */
  void onSuccess(T t);

  /**
   * Callback method invoked in the async thread when the request completes in failure.
   *
   * @param throwable the reason for the failure
   */
  void onFailure(Throwable throwable);
}

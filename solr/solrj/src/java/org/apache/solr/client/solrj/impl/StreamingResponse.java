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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.InputStream;

/**
 * Abstraction for streaming HTTP response handling in {@link ConcurrentUpdateBaseSolrClient}. This
 * allows the base class to work with different HTTP client implementations without direct
 * dependencies on specific client libraries (e.g., Jetty, JDK HttpClient).
 *
 * <p>Implementations should wrap the underlying HTTP client's response mechanism and provide access
 * to response status and body stream.
 */
public interface StreamingResponse extends AutoCloseable {

  /**
   * Wait for response headers to arrive and return the HTTP status code.
   *
   * @param timeoutMillis maximum time to wait in milliseconds
   * @return HTTP status code (e.g., 200, 404, 500)
   * @throws Exception if timeout occurs, connection fails, or other error
   */
  int awaitResponse(long timeoutMillis) throws Exception;

  /**
   * Get the response body as an InputStream for parsing.
   *
   * <p>This should be called after {@link #awaitResponse(long)} has successfully returned.
   *
   * @return response body stream, never null
   */
  InputStream getInputStream();

  /**
   * Get the underlying implementation-specific response object.
   *
   * <p>This is used by the {@link ConcurrentUpdateBaseSolrClient#onSuccess(Object, InputStream)}
   * hook method to allow subclasses access to implementation-specific metadata. The returned object
   * type depends on the HTTP client implementation being used.
   *
   * @return underlying response object (implementation-specific), may be null
   */
  Object getUnderlyingResponse();

  /**
   * Release resources associated with this response.
   *
   * @throws IOException if cleanup fails
   */
  @Override
  void close() throws IOException;
}

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
package org.apache.solr.client.solrj;

import java.io.IOException;
import java.io.Serial;

/**
 * Indicates that a request failed before any of it was written to the network, so the server cannot
 * have processed it. Retrying such a request on another node is safe even when it is not
 * idempotent.
 *
 * <p>Typically a pooled connection that the server had already closed.
 */
public class RequestNotSentException extends IOException {

  @Serial private static final long serialVersionUID = 1L;

  public RequestNotSentException(String message, Throwable cause) {
    super(message, cause);
  }
}

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
package org.apache.solr.handler.component;

/**
 * Test support for building {@link ShardResponse} instances that carry a failure. The failure
 * setter on {@link ShardResponse} is package-private (in production only {@link HttpShardHandler}
 * sets it), so tests in other packages build failed responses through this class instead of
 * widening that setter's visibility.
 */
public final class ShardResponseTestSupport {

  private ShardResponseTestSupport() {}

  /** A response to {@code request} that failed with {@code exception}. */
  public static ShardResponse failedResponse(ShardRequest request, Throwable exception) {
    ShardResponse response = new ShardResponse();
    response.setShardRequest(request);
    response.setException(exception);
    return response;
  }
}

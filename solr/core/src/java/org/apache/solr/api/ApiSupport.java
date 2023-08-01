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

package org.apache.solr.api;

import java.util.Collection;
import java.util.Collections;

/** The interface that is implemented by a request handler to support the V2 end point */
public interface ApiSupport {

  /**
   * Returns any (non-JAX-RS annotated) APIs associated with this request handler.
   *
   * @see #getJerseyResources()
   */
  Collection<Api> getApis();

  /**
   * Returns any JAX-RS annotated v2 APIs associated with this request handler.
   *
   * @see #getApis()
   */
  default Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return Collections.emptySet();
  }

  /** Whether this should be made available at the regular legacy path */
  default Boolean registerV1() {
    return Boolean.TRUE;
  }

  /** Whether this request handler must be made available at the /v2/ path */
  default Boolean registerV2() {
    return Boolean.FALSE;
  }
}

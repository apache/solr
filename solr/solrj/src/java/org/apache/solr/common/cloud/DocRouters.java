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

package org.apache.solr.common.cloud;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.common.SolrException;

public class DocRouters {

  private DocRouters() {}

  public static final String DEFAULT_NAME = CompositeIdRouter.NAME;

  public static final DocRouter DEFAULT;

  // currently just an implementation detail...
  private static final Map<String, DocRouter> routerMap;

  static {
    routerMap = new HashMap<>();
    PlainIdRouter plain = new PlainIdRouter();
    // instead of doing back compat this way, we could always convert the clusterstate on first read
    // to "plain" if it doesn't have any properties.
    routerMap.put(null, plain); // back compat with 4.0
    routerMap.put(PlainIdRouter.NAME, plain);
    routerMap.put(CompositeIdRouter.NAME, new CompositeIdRouter());
    routerMap.put(ImplicitDocRouter.NAME, new ImplicitDocRouter());
    // NOTE: careful that the map keys (the static .NAME members) are filled in by making them final

    DEFAULT = routerMap.get(DEFAULT_NAME);
  }

  public static DocRouter getDocRouter(String routerName) {
    DocRouter router = routerMap.get(routerName);
    if (router != null) return router;
    throw new SolrException(
        SolrException.ErrorCode.SERVER_ERROR, "Unknown document router '" + routerName + "'");
  }
}

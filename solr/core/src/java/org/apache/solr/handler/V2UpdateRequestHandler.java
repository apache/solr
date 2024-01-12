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

package org.apache.solr.handler;

import java.util.Collection;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.handler.admin.api.UpdateAPI;

/**
 * An extension of {@link UpdateRequestHandler} used solely to register the v2 /update APIs
 *
 * <p>At core-load time, Solr looks at each 'plugin' in ImplicitPlugins.json, fetches the v2 {@link
 * Api} implementations associated with each RequestHandler, and registers them in an {@link
 * org.apache.solr.api.ApiBag}. Since UpdateRequestHandler is mentioned multiple times in
 * ImplicitPlugins.json (once for each update API: /update, /update/json, etc.), this would cause
 * the v2 APIs to be registered in duplicate. To avoid this, Solr has this RequestHandler, whose
 * only purpose is to register the v2 APIs that conceptually should be associated with
 * UpdateRequestHandler.
 */
public class V2UpdateRequestHandler extends UpdateRequestHandler {

  @Override
  public Collection<Api> getApis() {
    return AnnotatedApi.getApis(new UpdateAPI(this));
  }

  @Override
  public Boolean registerV1() {
    return Boolean.FALSE;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}

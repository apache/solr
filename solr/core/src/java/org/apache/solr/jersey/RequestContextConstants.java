/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import com.codahale.metrics.Timer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Keys used to store and retrieve values from the Jersey request context.
 *
 * Properties are generally set in V2HttpCall's 'invokeJerseyRequest' and retrieved in individual
 * {@link javax.ws.rs.container.ContainerRequestFilter}s using {@link ContainerRequestContext#getProperty(String)}
 */
public class RequestContextConstants {
    private RequestContextConstants() {/* Private ctor prevents instantiation */}

    public static final String SOLR_QUERY_REQUEST_KEY = SolrQueryRequest.class.getName();
    public static final String HANDLER_METRICS_KEY = RequestHandlerBase.HandlerMetrics.class.getName();
    public static final String TIMER_KEY = Timer.Context.class.getName();
    public static final String SOLR_JERSEY_RESPONSE_KEY = SolrJerseyResponse.class.getName();
}

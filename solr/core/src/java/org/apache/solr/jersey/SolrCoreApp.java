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

import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import javax.inject.Singleton;
import java.util.Map;

/**
 * Jersey API registration point for an individual Solr core.
 *
 * General configuration properties are set in the constructor here, but API registration itself is deferred to core-load time, which invokes
 * {@link org.apache.solr.core.PluginBag#put(String, PluginBag.PluginHolder)} with each request-handler to register
 */
public class SolrCoreApp extends ResourceConfig {
    public SolrCoreApp(SolrCore solrCore) {
        super();
        setProperties(Map.of("jersey.config.server.tracing.type", "ALL", "jersey.config.server.tracing.threshold", "VERBOSE"));
        register(ApplicationEventLogger.class);
        register(RequestEventLogger.class);
        register(AllExceptionMapper.class);
        register(new AbstractBinder() {
            @Override
            protected void configure() {
                bindFactory(new SolrCoreFactory(solrCore))
                        .to(SolrCore.class)
                        .in(Singleton.class);
            }
        });
    }
}

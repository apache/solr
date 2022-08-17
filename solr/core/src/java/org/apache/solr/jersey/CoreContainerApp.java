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

import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.configsets.ListConfigSetsAPI;
import org.apache.solr.jersey.container.SomeAdminResource;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * CoreContainer-level (i.e. ADMIN) Jersey API registration.
 */
public class CoreContainerApp extends ResourceConfig {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public CoreContainerApp(CoreContainer coreContainer) {
        super();
        setProperties(Map.of("jersey.config.server.tracing.type", "ALL", "jersey.config.server.tracing.threshold", "VERBOSE"));
        register(ApplicationEventLogger.class);
        register(SolrRequestAuthorizer.class);
        register(SomeAdminResource.class);
        register(ListConfigSetsAPI.class);
        register(JavabinWriter.class);
        register(new AbstractBinder() {
            @Override
            protected void configure() {
                bindFactory(new CoreContainerFactory(coreContainer))
                        .to(CoreContainer.class)
                        .in(Singleton.class);
            }
        });
        //packages(true, "org.apache.solr.jersey.container");
    }

    public static class CoreContainerFactory implements Factory<CoreContainer> {

        private final CoreContainer singletonCC;

        public CoreContainerFactory(CoreContainer singletonCC) {
            this.singletonCC = singletonCC;
        }


        @Override
        public CoreContainer provide() {
            return singletonCC;
        }

        @Override
        public void dispose(CoreContainer instance) { /* No-op */ }
    }

    @Produces("application/javabin")
    public static class JavabinWriter implements MessageBodyWriter<ReflectMapWriter> {

        private final JavaBinCodec javaBinCodec;

        public JavabinWriter() {
            log.info("Creating new JavaBinWriter...hopefully this happens every request/response");
            this.javaBinCodec = new JavaBinCodec();
        }

        @Override
        public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
            log.info("Entered isWriteable with class={}, mediaType={}", type, mediaType);
            return mediaType.equals(new MediaType("application", "javabin"));
        }

        @Override
        public void writeTo(ReflectMapWriter reflectMapWriter, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
            log.info("Entered writeTo with object={}, type={}, mediaType={}", reflectMapWriter, type, mediaType);
            javaBinCodec.marshal(reflectMapWriter, entityStream);
        }
    }
}

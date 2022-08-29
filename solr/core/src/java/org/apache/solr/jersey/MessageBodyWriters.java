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

import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriterUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.XMLResponseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Type;

import static org.apache.solr.jersey.RequestContextConstants.SOLR_QUERY_REQUEST_KEY;
import static org.apache.solr.jersey.RequestContextConstants.SOLR_QUERY_RESPONSE_KEY;


public class MessageBodyWriters {

    /**
     * Registers a JAX-RS {@link MessageBodyWriter} that's used to return a javabin response when the request contains
     * the "Accept: application/javabin" header.
     */
    @Produces("application/javabin")
    public static class JavabinMessageBodyWriter implements MessageBodyWriter<JacksonReflectMapWriter> {

        private final JavaBinCodec javaBinCodec;

        public JavabinMessageBodyWriter() {
            this.javaBinCodec = new JavaBinCodec();
        }

        @Override
        public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
            return mediaType.equals(new MediaType("application", "javabin"));
        }

        @Override
        public void writeTo(JacksonReflectMapWriter reflectMapWriter, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
            javaBinCodec.marshal(reflectMapWriter, entityStream);
        }
    }

    // TODO This pattern should be appropriate for any QueryResponseWriter supported elsewhere in Solr.  Write a base
    //  class to remove all duplication from these content-specific implementations.
    @Produces("application/xml")
    public static class XmlMessageBodyWriter implements MessageBodyWriter<JacksonReflectMapWriter> {

        private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private final XMLResponseWriter xmlWriter = new XMLResponseWriter();

        @Context
        public ResourceContext resourceContext;

        @Override
        public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
            return mediaType.equals(new MediaType("application", "xml"));
        }

        @Override
        public void writeTo(JacksonReflectMapWriter reflectMapWriter, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
            final ContainerRequestContext requestContext = resourceContext.getResource(ContainerRequestContext.class);
            final SolrQueryRequest solrQueryRequest = (SolrQueryRequest) requestContext.getProperty(SOLR_QUERY_REQUEST_KEY);
            final SolrQueryResponse solrQueryResponse = (SolrQueryResponse) requestContext.getProperty(SOLR_QUERY_RESPONSE_KEY);

            final Writer w = QueryResponseWriterUtil.buildWriter(entityStream, ContentStreamBase.getCharsetFromContentType(mediaType.toString()));
            V2ApiUtils.squashIntoSolrResponse(solrQueryResponse, reflectMapWriter);
            xmlWriter.write(w, solrQueryRequest, solrQueryResponse);
            w.flush();

        }
    }
}

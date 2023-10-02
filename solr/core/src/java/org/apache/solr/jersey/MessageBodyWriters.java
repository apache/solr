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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_RESPONSE;
import static org.apache.solr.response.QueryResponseWriter.CONTENT_TYPE_TEXT_UTF8;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.CSVResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.QueryResponseWriterUtil;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.XMLResponseWriter;

/**
 * A collection of thin Jersey shims around Solr's existing {@link QueryResponseWriter} interface
 */
public class MessageBodyWriters {

  // Jersey has a default MessageBodyWriter for JSON so we don't need to declare one here
  // Which other response-writer formats are worth carrying forward into v2?

  @Produces(MediaType.APPLICATION_XML)
  public static class XmlMessageBodyWriter extends BaseMessageBodyWriter
      implements MessageBodyWriter<Object> {
    @Override
    public QueryResponseWriter createResponseWriter() {
      return new XMLResponseWriter();
    }

    @Override
    public String getSupportedMediaType() {
      return MediaType.APPLICATION_XML;
    }
  }

  @Produces(BINARY_CONTENT_TYPE_V2)
  public static class JavabinMessageBodyWriter extends BaseMessageBodyWriter
      implements MessageBodyWriter<Object> {
    @Override
    public QueryResponseWriter createResponseWriter() {
      return new BinaryResponseWriter();
    }

    @Override
    public String getSupportedMediaType() {
      return BINARY_CONTENT_TYPE_V2;
    }
  }

  @Produces(RawResponseWriter.CONTENT_TYPE)
  public static class RawMessageBodyWriter extends BaseMessageBodyWriter
      implements MessageBodyWriter<Object> {
    @Override
    public QueryResponseWriter createResponseWriter() {
      return new RawResponseWriter();
    }

    @Override
    public String getSupportedMediaType() {
      return RawResponseWriter.CONTENT_TYPE;
    }
  }

  @Produces(CONTENT_TYPE_TEXT_UTF8)
  public static class CsvMessageBodyWriter extends BaseMessageBodyWriter
      implements MessageBodyWriter<Object> {
    @Override
    public QueryResponseWriter createResponseWriter() {
      return new CSVResponseWriter();
    }

    @Override
    public String getSupportedMediaType() {
      return CONTENT_TYPE_TEXT_UTF8;
    }
  }

  public abstract static class BaseMessageBodyWriter implements MessageBodyWriter<Object> {

    @Context protected ResourceContext resourceContext;
    private final QueryResponseWriter responseWriter = createResponseWriter();

    public abstract QueryResponseWriter createResponseWriter();

    public abstract String getSupportedMediaType();

    @Override
    public boolean isWriteable(
        Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
      return mediaType.equals(MediaType.valueOf(getSupportedMediaType()));
    }

    @Override
    public void writeTo(
        Object toWrite,
        Class<?> type,
        Type genericType,
        Annotation[] annotations,
        MediaType mediaType,
        MultivaluedMap<String, Object> httpHeaders,
        OutputStream entityStream)
        throws IOException, WebApplicationException {
      final ContainerRequestContext requestContext =
          resourceContext.getResource(ContainerRequestContext.class);
      final SolrQueryRequest solrQueryRequest =
          (SolrQueryRequest) requestContext.getProperty(SOLR_QUERY_REQUEST);
      final SolrQueryResponse solrQueryResponse =
          (SolrQueryResponse) requestContext.getProperty(SOLR_QUERY_RESPONSE);

      V2ApiUtils.squashIntoSolrResponseWithHeader(solrQueryResponse, toWrite);
      QueryResponseWriterUtil.writeQueryResponse(
          entityStream, responseWriter, solrQueryRequest, solrQueryResponse, mediaType.toString());
    }
  }
}

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

package org.apache.solr.jersey;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;

/** A collection point for various {@link MessageBodyReader} implementations. */
public class MessageBodyReaders {

  /**
   * A JSON {@link MessageBodyReader} that caches request bodies for use later in the request
   * lifecycle.
   *
   * @see CachingDelegatingMessageBodyReader
   */
  @Provider
  @Consumes(MediaType.APPLICATION_JSON)
  public static class CachingJsonMessageBodyReader extends CachingDelegatingMessageBodyReader
      implements MessageBodyReader<Object> {
    @Override
    public MessageBodyReader<Object> getDelegate() {
      return new JacksonJsonProvider();
    }
  }

  /**
   * Caches the deserialized request body in the {@link ContainerRequestContext} for use later in
   * the request lifecycle.
   *
   * <p>This makes the request body accessible to any Jersey response filters or interceptors.
   *
   * @see PostRequestLoggingFilter
   */
  public abstract static class CachingDelegatingMessageBodyReader
      implements MessageBodyReader<Object> {
    public static final String DESERIALIZED_REQUEST_BODY_KEY = "request-body";

    @Context ServiceLocator serviceLocator;
    private final MessageBodyReader<Object> delegate;

    public CachingDelegatingMessageBodyReader() {
      this.delegate = getDelegate();
    }

    public abstract MessageBodyReader<Object> getDelegate();

    @Override
    public boolean isReadable(
        Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
      return delegate.isReadable(type, genericType, annotations, mediaType);
    }

    @Override
    public Object readFrom(
        Class<Object> type,
        Type genericType,
        Annotation[] annotations,
        MediaType mediaType,
        MultivaluedMap<String, String> httpHeaders,
        InputStream entityStream)
        throws IOException, WebApplicationException {
      final ContainerRequestContext requestContext = getRequestContext();
      final Object object =
          delegate.readFrom(type, genericType, annotations, mediaType, httpHeaders, entityStream);
      if (requestContext != null) {
        requestContext.setProperty(DESERIALIZED_REQUEST_BODY_KEY, object);
      }
      return object;
    }

    private ContainerRequestContext getRequestContext() {
      return serviceLocator.getService(ContainerRequestContext.class);
    }
  }
}

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
package org.apache.solr.client.solrj.impl;

import java.io.Closeable;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.solr.common.util.BufferedBytesOutputStream;
import org.eclipse.jetty.client.AsyncContentProvider;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.DeferredContentProvider;
import org.eclipse.jetty.util.Callback;

/**
 * A {@link ContentProvider} that provides content asynchronously through an {@link OutputStream} similar to {@link
 * DeferredContentProvider}.
 * <p>
 * {@link SolrOutputStreamContentProvider} can only be used in conjunction with {@link
 * Request#send(Response.CompleteListener)} (and not with its blocking counterpart {@link Request#send()}) because it
 * provides content asynchronously.
 * <p>
 * The deferred content is provided once by writing to the {@link #getOutputStream() output stream} and then fully
 * consumed. Invocations to the {@link #iterator()} method after the first will return an "empty" iterator because the
 * stream has been consumed on the first invocation. However, it is possible for subclasses to support multiple
 * invocations of {@link #iterator()} by overriding {@link #write(ByteBuffer)} and {@link #close()}, copying the bytes
 * and making them available for subsequent invocations.
 * <p>
 * Content must be provided by writing to the {@link #getOutputStream() output stream}, that must be {@link
 * OutputStream#close() closed} when all content has been provided.
 * <p>
 * Example usage:
 * <pre>
 * HttpClient httpClient = ...;
 *
 * // Use try-with-resources to autoclose the output stream
 * OutputStreamContentProvider content = new OutputStreamContentProvider();
 * try (OutputStream output = content.getOutputStream())
 * {
 *     httpClient.newRequest("localhost", 8080)
 *             .content(content)
 *             .send(new Response.CompleteListener()
 *             {
 *                 &#64;Override
 *                 public void onComplete(Result result)
 *                 {
 *                     // Your logic here
 *                 }
 *             });
 *
 *     // At a later time...
 *     output.write("some content".getBytes());
 * }
 * </pre>
 */
public class SolrOutputStreamContentProvider implements AsyncContentProvider, Callback, Closeable {
  private final DeferredContentProvider deferred = new DeferredContentProvider();

  private final BufferedBytesOutputStream out;

  public SolrOutputStreamContentProvider(ByteBuffer buffer) {
    this.out = new BufferedBytesOutputStream(buffer, deferred);
  }

  @Override
  public InvocationType getInvocationType() {
    return deferred.getInvocationType();
  }

  @Override
  public long getLength() {
    return deferred.getLength();
  }

  @Override
  public Iterator<ByteBuffer> iterator() {
    return deferred.iterator();
  }

  @Override
  public void setListener(Listener listener) {
    deferred.setListener(listener);
  }

  public OutputStream getOutputStream() {
    return out;
  }

  public void write(ByteBuffer buffer) {
    deferred.offer(buffer);
  }

  @Override
  public void close() {
    deferred.close();
  }

  @Override
  public void succeeded() {
    deferred.succeeded();
  }

  @Override
  public void failed(Throwable failure) {
    deferred.failed(failure);
  }

}

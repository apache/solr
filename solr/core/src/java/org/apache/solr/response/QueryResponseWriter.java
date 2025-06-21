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
package org.apache.solr.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import net.jcip.annotations.ThreadSafe;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Used to format responses to the client (not necessarily a "query").
 *
 * <p>Different <code>QueryResponseWriter</code>s are registered with the <code>SolrCore</code>. One
 * way to register a QueryResponseWriter with the core is through the <code>solrconfig.xml</code>
 * file.
 *
 * <p>Example <code>solrconfig.xml</code> entry to register a <code>QueryResponseWriter</code>
 * implementation to handle all queries with a writer type of "simple":
 *
 * <p><code>
 *    &lt;queryResponseWriter name="simple" class="foo.SimpleResponseWriter" /&gt;
 * </code>
 *
 * <p>A single instance of any registered QueryResponseWriter is created via the default constructor
 * and is reused for all relevant queries.
 */
@ThreadSafe
public interface QueryResponseWriter extends NamedListInitializedPlugin {
  public static String CONTENT_TYPE_XML_UTF8 = "application/xml; charset=UTF-8";
  public static String CONTENT_TYPE_TEXT_UTF8 = "text/plain; charset=UTF-8";

  /**
   * Writes the response to the {@link OutputStream}. {@code contentType} is from {@link
   * #getContentType(SolrQueryRequest, SolrQueryResponse)}, and it's often ignored.
   */
  void write(
      OutputStream out, SolrQueryRequest request, SolrQueryResponse response, String contentType)
      throws IOException;

  // should be "final" if interfaces allowed that
  default void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse response)
      throws IOException {
    write(out, request, response, getContentType(request, response));
  }

  /**
   * Return the applicable Content Type for a request, this method must be thread safe.
   *
   * <p>QueryResponseWriter's must implement this method to return a valid HTTP Content-Type header
   * for the request, that will logically correspond with the output produced by the write method.
   *
   * @return a Content-Type string, which may not be null.
   */
  String getContentType(SolrQueryRequest request, SolrQueryResponse response);

  /** Writes a String for debugging / testing purposes. */
  default String writeToString(SolrQueryRequest request, SolrQueryResponse response)
      throws IOException {
    var buffer = new ByteArrayOutputStream(32000);
    write(buffer, request, response);
    return buffer.toString(StandardCharsets.UTF_8);
  }
}

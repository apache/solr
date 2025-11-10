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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.FastWriter;
import org.apache.solr.request.SolrQueryRequest;

/** A writer supporting character streams ({@link Writer} based). */
public interface TextQueryResponseWriter extends QueryResponseWriter {

  /**
   * Write a SolrQueryResponse in a textual manner.
   *
   * <p>Information about the request (in particular: formatting options) may be obtained from
   * <code>req</code> but the dominant source of information should be <code>rsp</code>.
   *
   * <p>There are no mandatory actions that write must perform. An empty write implementation would
   * fulfill all interface obligations.
   */
  void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response)
      throws IOException;

  @Override
  default String writeToString(SolrQueryRequest request, SolrQueryResponse response)
      throws IOException {
    StringWriter sw = new StringWriter(32000);
    write(sw, request, response);
    return sw.toString();
  }

  @Override
  default void write(
      OutputStream outputStream,
      SolrQueryRequest request,
      SolrQueryResponse response,
      String contentType)
      throws IOException {
    OutputStream out = new NonFlushingStream(outputStream);
    Writer writer = buildWriter(out, ContentStreamBase.getCharsetFromContentType(contentType));
    write(writer, request, response);
    writer.flush();
  }

  private static Writer buildWriter(OutputStream outputStream, String charset)
      throws UnsupportedEncodingException {
    // note: OutputStreamWriter has an internal buffer; flush is needed
    Writer writer =
        (charset == null)
            ? new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
            : new OutputStreamWriter(outputStream, charset);

    return new FastWriter(writer); // note: buffered; therefore we need to call flush()
  }

  /**
   * Delegates write methods to an underlying {@link OutputStream}, but does not delegate {@link
   * OutputStream#flush()}, (nor {@link OutputStream#close()}). This allows code writing to this
   * stream to flush internal buffers without flushing the response. If we were to flush the
   * response early, that would trigger chunked encoding.
   *
   * <p>See SOLR-8669.
   */
  // TODO instead do in ServletUtils.closeShield(HttpServletResponse)
  class NonFlushingStream extends OutputStream {
    private final OutputStream outputStream;

    public NonFlushingStream(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public void write(int b) throws IOException {
      outputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      outputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      outputStream.write(b, off, len);
    }
  }
}

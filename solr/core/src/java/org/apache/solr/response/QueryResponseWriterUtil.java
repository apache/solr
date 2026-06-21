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
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import org.agrona.MutableDirectBuffer;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.ExpandableBuffers;
import org.apache.solr.common.util.ExpandableDirectBufferOutputStream;
import org.apache.solr.request.SolrQueryRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Static utility methods relating to {@link QueryResponseWriter}s
 */
public final class QueryResponseWriterUtil {
  private QueryResponseWriterUtil() { /* static helpers only */ }

  /**
   * Writes the response writer's result to the given output stream.
   * This method inspects the specified writer to determine if it is a 
   * {@link BinaryQueryResponseWriter} or not to delegate to the appropriate method.
   * @see BinaryQueryResponseWriter#write(OutputStream,SolrQueryRequest,SolrQueryResponse)
   * @see BinaryQueryResponseWriter#write(Writer,SolrQueryRequest,SolrQueryResponse)
   */
  public static ExpandableDirectBufferOutputStream writeQueryResponse(QueryResponseWriter responseWriter, SolrQueryRequest solrRequest,
      SolrQueryResponse solrResponse, HttpServletRequest request, HttpServletResponse response, String contentType) throws IOException {


    MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);//ExpandableBuffers.buffer1.get();

    ExpandableDirectBufferOutputStream outStream = new ExpandableDirectBufferOutputStream(expandableBuffer1);

    if (request != null) {
      request.setAttribute("responseBuffer", expandableBuffer1);
    }

    if (responseWriter instanceof BinaryQueryResponseWriter) {
      BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) responseWriter;
      binWriter.write(outStream, solrRequest, solrResponse);

    } else {
      Writer writer = buildWriter(outStream, ContentStreamBase.getCharsetFromContentType(contentType));
      responseWriter.write(writer, solrRequest, solrResponse);
      writer.close();
    }



    return outStream;
  }

  public static OutputStream writeQueryResponse(OutputStream out, QueryResponseWriter responseWriter, SolrQueryRequest solrRequest,
      SolrQueryResponse solrResponse, String contentType) throws IOException {

    if (responseWriter instanceof BinaryQueryResponseWriter) {
      BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) responseWriter;
      binWriter.write(out, solrRequest, solrResponse);

    } else {
      Writer writer = buildWriter(out, ContentStreamBase.getCharsetFromContentType(contentType));
      responseWriter.write(writer, solrRequest, solrResponse);
      writer.close();
    }

    return out;
  }
  
  private static Writer buildWriter(OutputStream outputStream, String charset) throws UnsupportedEncodingException {

    return (charset == null) ? new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
        : new OutputStreamWriter(outputStream, charset);
  }
}

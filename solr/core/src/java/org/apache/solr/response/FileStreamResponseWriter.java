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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.solr.client.solrj.response.JavaBinResponseParser;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.api.ReplicationAPIBase;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Response writer for file streaming operations, used for replication, exports, and other core Solr
 * operations.
 *
 * <p>This writer handles streaming of large files (such as index files) by looking for a {@link
 * org.apache.solr.core.SolrCore.RawWriter} object in the response under the {@link
 * ReplicationAPIBase#FILE_STREAM} key. When found, it delegates directly to the raw writer to
 * stream the file content efficiently.
 *
 * <p>This writer is specifically designed for replication file transfers and provides no fallback
 * behavior - it only works when a proper RawWriter is present in the response.
 */
public class FileStreamResponseWriter implements QueryResponseWriter {

  @Override
  public void write(
      OutputStream out, SolrQueryRequest request, SolrQueryResponse response, String contentType)
      throws IOException {
    SolrCore.RawWriter rawWriter =
        (SolrCore.RawWriter) response.getValues().get(ReplicationAPIBase.FILE_STREAM);
    if (rawWriter != null) {
      rawWriter.write(out);
      if (rawWriter instanceof Closeable closeable) {
        closeable.close();
      }
    }
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    SolrCore.RawWriter rawWriter =
        (SolrCore.RawWriter) response.getValues().get(ReplicationAPIBase.FILE_STREAM);
    if (rawWriter != null) {
      String contentType = rawWriter.getContentType();
      if (contentType != null) {
        return contentType;
      }
    }
    return JavaBinResponseParser.JAVABIN_CONTENT_TYPE;
  }
}

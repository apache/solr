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
package org.apache.solr.core;

import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.CSVResponseWriter;
import org.apache.solr.response.GeoJSONResponseWriter;
import org.apache.solr.response.GraphMLResponseWriter;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.PHPResponseWriter;
import org.apache.solr.response.PHPSerializedResponseWriter;
import org.apache.solr.response.PythonResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.RubyResponseWriter;
import org.apache.solr.response.SchemaXmlResponseWriter;
import org.apache.solr.response.SmileResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.XMLResponseWriter;

public class ResponseWriters {
  private static final SolrConfig.SolrPluginInfo info =
      SolrConfig.classVsSolrPluginInfo.get(QueryResponseWriter.class.getName());
  private static final QueryResponseWriter standard = new JSONResponseWriter();

  public static QueryResponseWriter get(String name) {
    PluginBag.PluginHolder<QueryResponseWriter> result = DEFAULT_RESPONSE_WRITER_HOLDERS.get(name);
    return result == null ? null : result.get();
  }

  public static QueryResponseWriter getOrDefault(String name) {
    PluginBag.PluginHolder<QueryResponseWriter> result = DEFAULT_RESPONSE_WRITER_HOLDERS.get(name);
    return result == null ? standard : result.get();
  }

  public static final Map<String, PluginBag.PluginHolder<QueryResponseWriter>>
      DEFAULT_RESPONSE_WRITER_HOLDERS;

  static {
    PluginBag.PluginHolder<QueryResponseWriter> json = wrap(standard);
    ImmutableMap.Builder<String, PluginBag.PluginHolder<QueryResponseWriter>> m =
        ImmutableMap.builder();
    m.put("xml", wrap(new XMLResponseWriter()));
    m.put(CommonParams.JSON, json);
    m.put("standard", json);
    m.put("geojson", wrap(new GeoJSONResponseWriter()));
    m.put("graphml", wrap(new GraphMLResponseWriter()));
    m.put("python", wrap(new PythonResponseWriter()));
    m.put("php", wrap(new PHPResponseWriter()));
    m.put("phps", wrap(new PHPSerializedResponseWriter()));
    m.put("ruby", wrap(new RubyResponseWriter()));
    m.put("raw", wrap(new RawResponseWriter()));
    m.put(CommonParams.JAVABIN, wrap(new BinaryResponseWriter()));
    m.put("csv", wrap(new CSVResponseWriter()));
    m.put("schema.xml", wrap(new SchemaXmlResponseWriter()));
    m.put("smile", wrap(new SmileResponseWriter()));
    m.put(ReplicationHandler.FILE_STREAM, wrap(getFileStreamWriter()));
    try {
      m.put(
          "xlsx",
          wrap(
              (QueryResponseWriter)
                  Class.forName("org.apache.solr.handler.extraction.XLSXResponseWriter")
                      .getConstructor()
                      .newInstance()));
    } catch (Exception e) {
      // don't worry; extraction module not in class path
    }
    DEFAULT_RESPONSE_WRITER_HOLDERS = m.build();
  }

  private static PluginBag.PluginHolder<QueryResponseWriter> wrap(QueryResponseWriter v) {
    return new PluginBag.PluginHolder<>(v, info);
  }

  private static BinaryResponseWriter getFileStreamWriter() {
    return new BinaryResponseWriter() {
      @Override
      public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response)
          throws IOException {
        RawWriter rawWriter = (RawWriter) response.getValues().get(ReplicationHandler.FILE_STREAM);
        if (rawWriter != null) {
          rawWriter.write(out);
          if (rawWriter instanceof Closeable) ((Closeable) rawWriter).close();
        }
      }

      @Override
      public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
        RawWriter rawWriter = (RawWriter) response.getValues().get(ReplicationHandler.FILE_STREAM);
        if (rawWriter != null) {
          return rawWriter.getContentType();
        } else {
          return BinaryResponseParser.BINARY_CONTENT_TYPE;
        }
      }
    };
  }

  public interface RawWriter {
    default String getContentType() {
      return BinaryResponseParser.BINARY_CONTENT_TYPE;
    }

    void write(OutputStream os) throws IOException;
  }

  public static PluginBag<QueryResponseWriter> constructBag(SolrCore core) {
    return new PluginBag<>(
        QueryResponseWriter.class,
        core,
        false,
        ResponseWriters.DEFAULT_RESPONSE_WRITER_HOLDERS,
        ResponseWriters.info);
  }
}

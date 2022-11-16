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
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ResponseWriters {
    public static final SolrConfig.SolrPluginInfo info =  SolrConfig.classVsSolrPluginInfo.get(QueryResponseWriter.class.getName());
    private static QueryResponseWriter standard;

    public static QueryResponseWriter get(String name) {
        return DEFAULT_RESPONSE_WRITERS.get(name);
    }
    public static QueryResponseWriter getOrDefault(String name){
        QueryResponseWriter result = DEFAULT_RESPONSE_WRITERS.get(name);
        return result==null? standard: result;

    }

    public static final Map<String, QueryResponseWriter> DEFAULT_RESPONSE_WRITERS;
    public static final Map<String, PluginBag.PluginHolder<QueryResponseWriter>> DEFAULT_RESPONSE_WRITER_HOLDERS ;


    static {
        HashMap<String, QueryResponseWriter> m = new HashMap<>(15, 1);
        m.put("xml", new XMLResponseWriter());
        m.put(CommonParams.JSON, new JSONResponseWriter());
        m.put("standard", m.get(CommonParams.JSON));
        m.put("geojson", new GeoJSONResponseWriter());
        m.put("graphml", new GraphMLResponseWriter());
        m.put("python", new PythonResponseWriter());
        m.put("php", new PHPResponseWriter());
        m.put("phps", new PHPSerializedResponseWriter());
        m.put("ruby", new RubyResponseWriter());
        m.put("raw", new RawResponseWriter());
        m.put(CommonParams.JAVABIN, new BinaryResponseWriter());
        m.put("csv", new CSVResponseWriter());
        m.put("schema.xml", new SchemaXmlResponseWriter());
        m.put("smile", new SmileResponseWriter());
        standard = m.get("standard");
        m.put(ReplicationHandler.FILE_STREAM, getFileStreamWriter());
        DEFAULT_RESPONSE_WRITERS = Collections.unmodifiableMap(m);
        try {
            m.put(
                    "xlsx",
                    (QueryResponseWriter)
                            Class.forName("org.apache.solr.handler.extraction.XLSXResponseWriter")
                                    .getConstructor()
                                    .newInstance());
        } catch (Exception e) {
            // don't worry; extraction module not in class path
        }
        ImmutableMap.Builder<String, PluginBag.PluginHolder<QueryResponseWriter>> b = ImmutableMap.builder();
        DEFAULT_RESPONSE_WRITERS.forEach((k, v) -> b.put(k, new PluginBag.PluginHolder<>(v, info)));
        DEFAULT_RESPONSE_WRITER_HOLDERS = b.build();
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
}

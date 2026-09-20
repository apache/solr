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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StreamParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;

/**
 * Queries a single Solr instance and maps SolrDocs to a Stream of Tuples.
 *
 * @since 5.1.0
 */
public class SolrStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private String baseUrl;
  private String core;
  private String path;
  private SolrParams params;
  private int numWorkers;
  private int workerID;
  private boolean trace;
  private Map<String, String> fieldMappings;
  private transient TupleStreamParser tupleStreamParser;
  private String slice;
  private long checkpoint = -1;
  private boolean distrib = true;
  private String user;
  private String password;

  private transient SolrClientCache clientCache;
  private transient boolean doCloseCache;

  /**
   * @param collectionOrCoreUrl URL of the Solr core or collection to query, typically of the form
   *     "http://host:8983/solr/myCore".
   * @param params query-parameters sent with the streaming request
   * @deprecated Use base URL constructor instead.
   */
  @Deprecated(since = "10.1")
  public SolrStream(String collectionOrCoreUrl, SolrParams params) {
    this.baseUrl = collectionOrCoreUrl;
    this.params = params;
  }

  // TODO SOLR-17995 proposes that we should deprecate this constructor in favor of one of the other
  // constructors that requires users to provide the core as an explicit parameter
  /**
   * @param collectionOrCoreUrl URL of the Solr core or collection to query, typically of the form
   *     "http://host:8983/solr/myCore".
   * @param path the request handler path to query (e.g. "/export"). If not provided, defaults to
   *     "/select".
   * @param params query-parameters sent with the streaming request
   * @deprecated Use base URL constructor instead.
   */
  @Deprecated(since = "10.1")
  public SolrStream(String collectionOrCoreUrl, String path, SolrParams params) {
    this(collectionOrCoreUrl, null, path, params);
  }

  /**
   * @param baseUrl the Solr node's "base" URL (i.e. no core or collection in the path
   * @param params query-parameters sent with the streaming request
   * @param core the name of the collection or core to query; must be hosted at {@code baseUrl}
   */
  public SolrStream(String baseUrl, SolrParams params, String core) {
    this(baseUrl, core, null, params);
  }

  /**
   * @param baseUrl the Solr node's "base" URL (i.e. no core or collection in the path
   * @param core the name of the collection or core to query; must be hosted at {@code baseUrl}
   * @param path the request handler path to query (e.g. "/export"). If not provided (i.e. {@code
   *     null}), the handler is instead resolved from a "qt" param embedded in {@code params}, or
   *     defaults to "/select" if no such param is present.
   * @param params query-parameters sent with the streaming request
   */
  public SolrStream(String baseUrl, String core, String path, SolrParams params) {
    this.baseUrl = baseUrl;
    this.core = core;
    this.path = path;
    this.params = params;
  }

  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  @Override
  public List<TupleStream> children() {
    return new ArrayList<>();
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.distrib = !context.isLocal();
    this.numWorkers = context.numWorkers;
    this.workerID = context.workerID;
    this.clientCache = context.getSolrClientCache();
  }

  public void setCredentials(String user, String password) {
    this.user = user;
    this.password = password;
  }

  /** Opens the stream to a single Solr instance. */
  @Override
  public void open() throws IOException {
    if (clientCache == null) {
      doCloseCache = true;
      clientCache = new SolrClientCache();
    } else {
      doCloseCache = false;
    }

    try {
      SolrParams requestParams = loadParams(params);
      if (!distrib) {
        ((ModifiableSolrParams) requestParams).add("distrib", "false");
      }
      tupleStreamParser = constructParser(requestParams);
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException("params " + params, e);
    }
  }

  /** Setting trace to true will include the "_CORE_" field in each Tuple emitted by the stream. */
  public void setTrace(boolean trace) {
    this.trace = trace;
  }

  public void setSlice(String slice) {
    this.slice = slice;
  }

  public void setCheckpoint(long checkpoint) {
    this.checkpoint = checkpoint;
  }

  private ModifiableSolrParams loadParams(SolrParams paramsIn) throws IOException {
    ModifiableSolrParams solrParams = new ModifiableSolrParams(paramsIn);
    if (params.get("partitionKeys") != null) {
      if (!params.get("partitionKeys").equals("none") && numWorkers > 1) {
        String partitionFilter = getPartitionFilter();
        solrParams.add("fq", partitionFilter);
      }
    } else if (numWorkers > 1) {
      throw new IOException(
          "When numWorkers > 1 partitionKeys must be set. Set partitionKeys=none to send the entire stream to each worker.");
    }

    if (checkpoint > 0) {
      solrParams.add("fq", "{!frange cost=100 incl=false l=" + checkpoint + "}_version_");
    }

    return solrParams;
  }

  private String getPartitionFilter() {
    StringBuilder buf = new StringBuilder("{!hash workers=");
    buf.append(this.numWorkers);
    buf.append(" worker=");
    buf.append(this.workerID);
    buf.append("}");
    return buf.toString();
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName("non-expressible")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_SOURCE)
        .withExpression("non-expressible");
  }

  /** Closes the Stream to a single Solr Instance */
  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(tupleStreamParser);
    if (doCloseCache) {
      IOUtils.closeQuietly(clientCache);
    }
  }

  /** Reads a Tuple from the stream. The Stream is completed when Tuple.EOF == true. */
  @Override
  public Tuple read() throws IOException {
    try {
      Map<String, Object> fields = tupleStreamParser.next();

      if (fields == null) {
        // Return the EOF tuple.
        return Tuple.EOF();
      } else {

        String msg = (String) fields.get(StreamParams.EXCEPTION);
        if (msg != null) {
          HandledException ioException = new HandledException(msg);
          throw ioException;
        }

        if (trace) {
          fields.put("_CORE_", this.baseUrl);
          if (slice != null) {
            fields.put("_SLICE_", slice);
          }
        }

        if (fieldMappings != null) {
          fields = mapFields(fields, fieldMappings);
        }
        return new Tuple(fields);
      }
    } catch (HandledException e) {
      throw new IOException("--> " + this.baseUrl + ":" + e.getMessage());
    } catch (Exception e) {
      // The Stream source did not provide an exception in a format that the SolrStream could
      // propagate.
      throw new IOException(
          "--> "
              + this.baseUrl
              + ": An exception has occurred on the server, refer to server log for details.",
          e);
    }
  }

  public void setDistrib(boolean distrib) {
    this.distrib = distrib;
  }

  public boolean getDistrib() {
    return distrib;
  }

  public static class HandledException extends IOException {
    public HandledException(String msg) {
      super(msg);
    }
  }

  /** There is no known sort applied to a SolrStream */
  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  private <V> Map<String, V> mapFields(Map<String, V> fields, Map<String, String> mappings) {

    Iterator<Map.Entry<String, String>> it = mappings.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String mapFrom = entry.getKey();
      String mapTo = entry.getValue();
      V v = fields.get(mapFrom);
      fields.remove(mapFrom);
      fields.put(mapTo, v);
    }

    return fields;
  }

  private TupleStreamParser constructParser(SolrParams requestParams)
      throws IOException, SolrServerException {
    // performance optimization - remove extra whitespace when streaming
    requestParams = SolrParams.wrapDefaults(requestParams, SolrParams.of("indent", "off"));

    QueryRequest query =
        path == null
            ? new QueryRequest(requestParams, SolrRequest.METHOD.POST)
            : new QueryRequest(path, requestParams, SolrRequest.METHOD.POST);
    String wt = requestParams.get(CommonParams.WT, "json");
    query.setResponseParser(new InputStreamResponseParser(wt));

    if (user != null && password != null) {
      query.setBasicAuthCredentials(user, password);
    }

    var client = clientCache.getHttpSolrClient(baseUrl);
    NamedList<Object> genericResponse = client.request(query, core);
    InputStream stream = (InputStream) genericResponse.get(InputStreamResponseParser.STREAM_KEY);
    // since 9.4 the updated format has a dedicated status field
    final Integer statusCode = (Integer) genericResponse.get("responseStatus");

    if (statusCode == 401
        || statusCode == 403) { // auth response comes as html, so propagate as string
      String errMsg = consumeStreamAsErrorMessage(stream);
      throw new IOException(
          "Query to '"
              + query.getPath()
              + "?"
              + query.getParams()
              + "' failed due to: ("
              + statusCode
              + ") "
              + errMsg);
    }

    if (CommonParams.JAVABIN.equals(wt)) {
      return new JavabinTupleStreamParser(stream, true);
    } else {
      InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
      return new JSONTupleStream(reader);
    }
  }

  private String consumeStreamAsErrorMessage(InputStream stream) throws IOException {
    StringBuilder errMsg = new StringBuilder();
    int r;
    char[] ach = new char[1024];
    if (stream != null) {
      try (InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
        while ((r = reader.read(ach)) != -1) errMsg.append(ach, 0, r);
      }
    }
    return errMsg.toString();
  }
}

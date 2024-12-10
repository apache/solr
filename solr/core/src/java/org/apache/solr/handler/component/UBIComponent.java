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
package org.apache.solr.handler.component;

import static org.apache.solr.handler.RequestHandlerBase.isInternalShardRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.DefaultStreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.LoggingStream;
import org.apache.solr.response.ResultContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User Behavior Insights (UBI) is an open standard for gathering query and event data from users
 * and storing it in a structured format. UBI can be used for in session personalization, implicit
 * judgements, powering recommendation systems among others. Learn more about the UBI standard at <a
 * href="https://ubisearch.dev">https://ubisearch.dev</a>.
 *
 * <p>The response from Solr is augmented by this component, and optionally the query details can be
 * tracked and logged to various systems including log files or other backend systems.
 *
 * <p>Data tracked is a unique query_id for the search request, the end user's query, metadata about
 * the query as a JSON map, and the resulting document id's.
 *
 * <p>You provide a streaming expression that is parsed and loaded by the component to stream query
 * data to a target of your choice. If you do not, then the default expression of
 * 'logging(ubi_queries.jsonl,ubiQuery())"' is used which logs data to
 * $SOLR_HOME/userfiles/ubi_queries.jsonl file.
 *
 * <p>You must source your streaming events using the 'ubiQuery()' streaming expression to retrieve
 * the {@link UBIQuery} object that contains the data for recording.
 *
 * <p>Event data is tracked by letting the user write events directly to the event repository of
 * your choice, it could be a Solr collection, it could be a file or S3 bucket, and that is NOT
 * handled by this component.
 *
 * <p>Add the component to a requestHandler in solrconfig.xml like this:
 *
 * <pre class="prettyprint">
 * &lt;searchComponent name="ubi" class="solr.UBIComponent"/&gt;
 *
 * &lt;requestHandler name="/select" class="solr.SearchHandler"&gt;
 *   &lt;lst name="defaults"&gt;
 *
 *     ...
 *
 *   &lt;/lst&gt;
 *   &lt;arr name="components"&gt;
 *     &lt;str&gt;ubi&lt;/str&gt;
 *   &lt;/arr&gt;
 * &lt;/requestHandler&gt;</pre>
 *
 * It can then be enabled at query time by supplying
 *
 * <pre>ubi=true</pre>
 *
 * query parameter.
 *
 * <p>Ideally this component is used with the JSON Query syntax, as that facilitates passing in the
 * additional data to be tracked with a query. Here is an example:
 *
 * <pre>
 *     {
 *     "query" : "apple AND ipod",
 *     "limit":2,
 *     "start":2,
 *     "filter": [
 *        "inStock:true"
 *      ]
 *     params: {
 *       "ubi": "true"
 *       "user_query": "Apple iPod",
 *       "query_attributes": {
 *         "experiment_name": "super_secret",
 *         "page": 2,
 *         "in_stock": "true"
 *       }
 *     }
 *   }
 * </pre>
 *
 * <p>Refer to <a
 * href="https://solr.apache.org/guide/solr/latest/query-guide/user-behavior-insights.html">
 * https://solr.apache.org/guide/solr/latest/query-guide/user-behavior-insights.html</a> for more
 * details
 */
public class UBIComponent extends SearchComponent implements SolrCoreAware {

  public static final String COMPONENT_NAME = "ubi";
  public static final String QUERY_ID = "query_id";
  public static final String QUERY_ATTRIBUTES = "query_attributes";
  public static final String USER_QUERY = "user_query";
  public static final String APPLICATION = "application";
  public static final String DOC_IDS = "doc_ids";

  protected PluginInfo info = PluginInfo.EMPTY_INFO;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private StreamContext streamContext;
  private StreamExpression streamExpression;
  private StreamFactory streamFactory;

  protected SolrParams initArgs;

  @Override
  public void init(NamedList<?> args) {
    this.initArgs = args.toSolrParams();
  }

  @Override
  public void inform(SolrCore core) {
    log.info("Initializing UBIComponent");

    CoreContainer coreContainer = core.getCoreContainer();
    SolrClientCache solrClientCache = coreContainer.getSolrClientCache();

    String expr;
    String queryProcessingExpression = null;
    if (initArgs != null) {
      queryProcessingExpression = initArgs.get("queryProcessingExpression");
    }

    if (queryProcessingExpression == null) {
      log.info(
          "No 'queryProcessingExpression' file provided to describe processing of UBI query information.");
      log.info(
          "Writing out UBI query information to local $SOLR_HOME/userfiles/ubi_queries.jsonl file instead.");
      // Most simplistic version
      // expr = "logging(ubi_queries.jsonl, tuple(query_id=49,user_query=\"RAM memory\"))";

      // The default version
      expr = "logging(ubi_queries.jsonl,ubiQuery())";

      // feels like 'stream' or 'get' or something should let me create a tuple out of something
      // in the streamContext.   That would turn the "ubi-query" object in the stream context into a
      // nice
      // tuple and return it.  streamContext(ubi-query)??
      // expr = "logging(ubi_queries.jsonl," + "get(ubi-query)" + ")";
    } else {

      String[] args = {}; // maybe we have variables?
      try (LineNumberReader bufferedReader =
          new LineNumberReader(
              new InputStreamReader(
                  core.getResourceLoader().openResource(queryProcessingExpression),
                  StandardCharsets.UTF_8))) {
        expr = readExpression(bufferedReader, args);
      } catch (IOException ioe) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Error reading file " + queryProcessingExpression,
            ioe);
      }
    }

    streamContext = new StreamContext();
    streamContext.put("solr-core", core);
    streamContext.setSolrClientCache(solrClientCache);

    streamExpression = StreamExpressionParser.parse(expr);
    if (!streamExpression.toString().contains("ubiQuery")) {
      log.error(
          "The streaming expression "
              + streamExpression
              + " must include the 'ubiQuery()' to record UBI queries.");
    }

    streamFactory = new DefaultStreamFactory();
    streamFactory.withFunctionName("logging", LoggingStream.class);
    streamFactory.withFunctionName("ubiQuery", UBIQueryStream.class);

    if (coreContainer.isZooKeeperAware()) {
      String defaultZkHost = core.getCoreContainer().getZkController().getZkServerAddress();
      streamFactory.withDefaultZkHost(defaultZkHost);
    }
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }
    rb.setNeedDocList(true);
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }
    if (!isInternalShardRequest(rb.req)) { // subordinate shard req shouldn't yield logs
      storeUbiDetails(
          rb,
          ubiQuery -> {
            try {
              DocList docList = ((ResultContext) rb.rsp.getResponse()).getDocList();
              ubiQuery.setDocIds(extractDocIds(docList, rb.req.getSearcher()));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return ResponseBuilder.STAGE_DONE;
    }

    if (rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
      return ResponseBuilder.STAGE_GET_FIELDS;
    }

    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      storeUbiDetails(
          rb,
          ubiQuery ->
              ubiQuery.setDocIds(
                  String.join(",", rb.resultIds.keySet().stream().map(Object::toString).toList())));
      return ResponseBuilder.STAGE_DONE;
    }

    return ResponseBuilder.STAGE_DONE;
  }

  private static UBIQuery getUbiQuery(ResponseBuilder rb) {
    if (rb.rsp.getValues().get("ubi") != null) {
      return null;
    }
    SolrParams params = rb.req.getParams();

    SolrIndexSearcher searcher = rb.req.getSearcher();

    String queryId = params.get(QUERY_ID);
    UBIQuery ubiQuery = new UBIQuery(queryId);

    ubiQuery.setUserQuery(params.get(USER_QUERY));
    ubiQuery.setApplication(params.get(APPLICATION));
    if (ubiQuery.getApplication() == null) {
      ubiQuery.setApplication(
          rb.isDistrib
              ? rb.req.getCloudDescriptor().getCollectionName()
              : searcher.getCore().getName());
    }

    String queryAttributes = params.get(QUERY_ATTRIBUTES);

    if (queryAttributes != null && queryAttributes.toString().startsWith("{")) {
      // Look up the original nested JSON format, typically passed in
      // via the JSON formatted query.
      @SuppressWarnings("rawtypes")
      Map jsonProperties = rb.req.getJSON();
      if (jsonProperties.containsKey("params")) {
        @SuppressWarnings("rawtypes")
        Map paramsProperties = (Map) jsonProperties.get("params");
        if (paramsProperties.containsKey(QUERY_ATTRIBUTES)) {
          @SuppressWarnings("rawtypes")
          Map queryAttributesAsMap = (Map) paramsProperties.get(QUERY_ATTRIBUTES);
          ubiQuery.setQueryAttributes(queryAttributesAsMap);
        }
      }
    }
    return ubiQuery;
  }

  private void storeUbiDetails(ResponseBuilder rb, Consumer<UBIQuery> docIdsSetter)
      throws IOException {
    UBIQuery ubiQuery = getUbiQuery(rb);
    if (ubiQuery == null) return;
    docIdsSetter.accept(ubiQuery);
    addUserBehaviorInsightsToResponse(ubiQuery, rb);
    recordQuery(ubiQuery);
  }

  private void recordQuery(UBIQuery ubiQuery) throws IOException {
    TupleStream stream;

    stream = constructStream(streamFactory, streamExpression);

    streamContext.put("ubi-query", ubiQuery);
    stream.setStreamContext(streamContext);

    getTuple(stream);
  }

  private void addUserBehaviorInsightsToResponse(UBIQuery ubiQuery, ResponseBuilder rb) {
    SimpleOrderedMap<String> ubiResponseInfo = new SimpleOrderedMap<>();

    ubiResponseInfo.add(QUERY_ID, ubiQuery.getQueryId());
    rb.rsp.add("ubi", ubiResponseInfo);
  }

  protected String extractDocIds(DocList dl, SolrIndexSearcher searcher) throws IOException {
    IndexSchema schema = searcher.getSchema();

    if (schema.getUniqueKeyField() == null) {
      log.error("Can't track documents for query without unique field.");
      return "";
    }
    StringBuilder sb = new StringBuilder();

    Set<String> fields = Collections.singleton(schema.getUniqueKeyField().getName());
    for (DocIterator iter = dl.iterator(); iter.hasNext(); ) {
      sb.append(schema.printableUniqueKey(searcher.getDocFetcher().doc(iter.nextDoc(), fields)))
          .append(',');
    }
    String docIds = sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";

    return docIds;
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for (; ; ) {
      Tuple t = tupleStream.read();
      if (t.EOF) {
        break;
      } else {
        tuples.add(t);
      }
    }
    tupleStream.close();
    return tuples;
  }

  protected Tuple getTuple(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    Tuple t = tupleStream.read();
    tupleStream.close();
    return t;
  }

  // this should be a shared utility method
  public static String readExpression(LineNumberReader bufferedReader, String[] args)
      throws IOException {

    StringBuilder exprBuff = new StringBuilder();

    boolean comment = false;
    while (true) {
      String line = bufferedReader.readLine();
      if (line == null) {
        break;
      }

      if (line.indexOf("/*") == 0) {
        comment = true;
        continue;
      }

      if (line.indexOf("*/") == 0) {
        comment = false;
        continue;
      }

      if (comment || line.startsWith("#") || line.startsWith("//")) {
        continue;
      }

      // Substitute parameters
      if (line.length() > 0) {
        for (int i = 1; i < args.length; i++) {
          String arg = args[i];
          line = line.replace("$" + i, arg);
        }
      }

      exprBuff.append(line);
    }

    return exprBuff.toString();
  }

  private static TupleStream constructStream(
      StreamFactory streamFactory, StreamExpression streamExpression) {
    try {
      return streamFactory.constructStream(streamExpression);
    } catch (IOException exception) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error constructing stream for processing UBI data collection using expression "
              + streamExpression,
          exception);
    }
  }

  @Override
  public String getDescription() {
    return "A component that tracks the original user query and the resulting documents returned to understand the user.";
  }

  /**
   * Handles all the data required for tracking a query using User Behavior Insights.
   *
   * <p>Compatible with the
   * https://github.com/o19s/ubi/blob/main/schema/1.2.0/query.request.schema.json.
   */
  public static class UBIQuery {

    private String application;
    private String queryId;
    private String userQuery;
    private Date timestamp;

    @SuppressWarnings("rawtypes")
    private Map queryAttributes;

    private String docIds;

    public UBIQuery(String queryId) {

      if (queryId == null) {
        queryId = UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
      }
      this.queryId = queryId;
      this.timestamp = new Date();
    }

    public Date getTimestamp() {
      return timestamp;
    }

    public void setApplication(String application) {
      this.application = application;
    }

    public String getApplication() {
      return this.application;
    }

    public String getQueryId() {
      return queryId;
    }

    public void setQueryId(String queryId) {
      this.queryId = queryId;
    }

    public String getUserQuery() {
      return userQuery;
    }

    public void setUserQuery(String userQuery) {
      this.userQuery = userQuery;
    }

    @SuppressWarnings("rawtypes")
    public Map getQueryAttributes() {
      return queryAttributes;
    }

    @SuppressWarnings("rawtypes")
    public void setQueryAttributes(Map queryAttributes) {
      this.queryAttributes = queryAttributes;
    }

    public String getDocIds() {
      return docIds;
    }

    public void setDocIds(String docIds) {
      this.docIds = docIds;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map toMap() {
      @SuppressWarnings({"rawtypes", "unchecked"})
      Map map = new HashMap();
      map.put(QUERY_ID, this.queryId);
      map.put(
          "timestamp",
          DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(this.timestamp.getTime())));
      if (this.application != null) {
        map.put(APPLICATION, this.application);
      }
      if (this.userQuery != null) {
        map.put(USER_QUERY, this.userQuery);
      }
      if (this.docIds != null) {
        map.put(DOC_IDS, this.docIds);
      }
      if (this.queryAttributes != null) {

        ObjectMapper objectMapper = new ObjectMapper();
        try {
          map.put(QUERY_ATTRIBUTES, objectMapper.writeValueAsString(this.queryAttributes));
        } catch (JsonProcessingException e) {
          // eat it.
        }
      }

      return map;
    }
  }

  /**
   * Converts a UBIQuery that is stored in the StreamContext under the key 'ubi-query' into a Tuple
   * and returns it.
   *
   * <p>I suspect that if I had the right magic with a LetStream or a GetStream, I could somehow
   * just use that to say "pluck the 'ubi-query' object out of the StreamContext and call .toTuple
   * or make a map of it and that would be my tuple'.
   */
  public static class UBIQueryStream extends TupleStream implements Expressible {

    private StreamContext streamContext;
    private boolean finished;

    public UBIQueryStream(StreamExpression expression, StreamFactory factory) throws IOException {}

    @Override
    public StreamExpression toExpression(StreamFactory factory) throws IOException {
      return toExpression(factory, true);
    }

    private StreamExpression toExpression(StreamFactory factory, boolean includeStreams)
        throws IOException {
      // function name
      StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

      return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
      explanation.setFunctionName(factory.getFunctionName(this.getClass()));
      explanation.setImplementingClass(this.getClass().getName());
      explanation.setExpressionType(Explanation.ExpressionType.STREAM_SOURCE);
      explanation.setExpression(toExpression(factory, false).toString());

      return explanation;
    }

    @Override
    public void setStreamContext(StreamContext context) {
      this.streamContext = context;
    }

    @Override
    public List<TupleStream> children() {
      List<TupleStream> l = new ArrayList<>();
      return l;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {}

    @SuppressWarnings({"unchecked"})
    @Override
    public Tuple read() throws IOException {

      if (finished) {
        return Tuple.EOF();
      } else {
        finished = true;

        UBIQuery ubiQuery = (UBIQuery) streamContext.get("ubi-query");

        return new Tuple(ubiQuery.toMap());
      }
    }

    /** Return the stream sort - ie, the order in which records are returned */
    @Override
    public StreamComparator getStreamSort() {
      return null;
    }

    @Override
    public int getCost() {
      return 0;
    }
  }
}

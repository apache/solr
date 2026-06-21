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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * Connects to Zookeeper to pick replicas from a specific collection to send the query to.
 * Under the covers the SolrStream instances send the query to the replicas.
 * SolrStreams are opened using a thread pool, but a single thread is used
 * to iterate and merge Tuples from each SolrStream.
 * @since 5.1.0
 **/

public class CloudSolrStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  public static final Slice[] SLICES = new Slice[0];
  private static final Pattern COMPILE = Pattern.compile("\\s+");

  protected String zkHost;
  protected String collection;
  protected ModifiableSolrParams params;
  protected Map<String, String> fieldMappings;
  protected StreamComparator comp;
  private boolean trace;
  protected transient volatile Map<String, Tuple> eofTuples;
  protected transient CloudHttp2SolrClient cloudSolrClient;
  protected transient volatile List<TupleStream> solrStreams;
  protected transient volatile TreeSet<TupleWrapper> tuples;
  protected transient StreamContext streamContext;

  // Used by parallel stream
  protected CloudSolrStream(){

  }

  /**
   * @param zkHost         Zookeeper ensemble connection string
   * @param collectionName Name of the collection to operate on
   * @param params         Map&lt;String, String[]&gt; of parameter/value pairs
   * @throws IOException Something went wrong
   */
  public CloudSolrStream(String zkHost, String collectionName, SolrParams params) throws IOException {
    init(collectionName, zkHost, params);
  }

  public CloudSolrStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = StreamFactory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = StreamFactory.getNamedOperands(expression);
    StreamExpressionNamedParameter aliasExpression = StreamFactory.getNamedOperand(expression, "aliases");
    StreamExpressionNamedParameter zkHostExpression = StreamFactory.getNamedOperand(expression, "zkHost");

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != 1 + namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - unknown operands found",expression));
    }

    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }

    ModifiableSolrParams mParams = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("aliases")){
        mParams.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    // Aliases, optional, if provided then need to split
    if(null != aliasExpression && aliasExpression.getParameter() instanceof StreamExpressionValue){
      fieldMappings = new HashMap<>();
      for(String mapping : ((StreamExpressionValue)aliasExpression.getParameter()).getValue().split(",")){
        String[] parts = mapping.trim().split("=");
        if(2 == parts.length){
          fieldMappings.put(parts[0], parts[1]);
        }
        else{
          throw new IOException(String.format(Locale.ROOT,"invalid expression %s - alias expected of the format origName=newName",expression));
        }
      }
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    }
    else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }

    // We've got all the required items
    init(collectionName, zkHost, mParams);
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    // functionName(collectionName, param1, param2, ..., paramN, sort="comp", [aliases="field=alias,..."])

    // function name
    StreamExpression expression = new StreamExpression("search");

    // collection
    if(collection.indexOf(',') > -1) {
      expression.addParameter("\""+collection+"\"");
    } else {
      expression.addParameter(collection);
    }

    for (Entry<String, String[]> param : params.getMap().entrySet()) {
      for (String val : param.getValue()) {
        // SOLR-8409: Escaping the " is a special case.
        // Do note that in any other BASE streams with parameters where a " might come into play
        // that this same replacement needs to take place.
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(),
            val.replace("\"", "\\\"")));
      }
    }

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    // aliases
    if(null != fieldMappings && 0 != fieldMappings.size()){
      StringBuilder sb = new StringBuilder();
      for(Entry<String,String> mapping : fieldMappings.entrySet()){
        if(sb.length() > 0){ sb.append(","); }
        sb.append(mapping.getKey());
        sb.append("=");
        sb.append(mapping.getValue());
      }

      expression.addParameter(new StreamExpressionNamedParameter("aliases", sb.toString()));
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

    explanation.setFunctionName("search");
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory).toString());

    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);

    if(null != params){
      ModifiableSolrParams mParams = new ModifiableSolrParams(params);
      child.setExpression(mParams.getMap().entrySet().stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
    }
    explanation.addChild(child);

    return explanation;
  }

  void init(String collectionName, String zkHost, SolrParams params) throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = new ModifiableSolrParams(params);

    // If the comparator is null then it was not explicitly set so we will create one using the sort parameter
    // of the query. While doing this we will also take into account any aliases such that if we are sorting on
    // fieldA but fieldA is aliased to alias.fieldA then the comparator will be against alias.fieldA.

    if (params.get("q") == null) {
      throw new IOException("q param expected for search function");
    }

    if (params.getParams("fl") == null) {
      throw new IOException("fl param expected for search function");
    }
    String fls = String.join(",", params.getParams("fl"));

    if (params.getParams(SORT) == null) {
      throw new IOException("sort param expected for search function");
    }
    String sorts = String.join(",", params.getParams(SORT));
    this.comp = parseComp(sorts, fls);
  }

  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  public void setTrace(boolean trace) {
    this.trace = trace;
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  /**
  * Opens the CloudSolrStream
  *
  ***/
  public synchronized void open() throws IOException {
    this.tuples = new TreeSet();
    this.solrStreams = Collections.synchronizedList(new ArrayList());
    this.eofTuples = new ConcurrentHashMap<>();
    constructStreams();
    openStreams();
  }


  public Map getEofTuples() {
    return this.eofTuples;
  }

  public List<TupleStream> children() {
    return solrStreams;
  }

  private StreamComparator parseComp(String sort, String fl) throws IOException {

    String[] fls = fl.split(",");
    HashSet fieldSet = new HashSet();
    for(String f : fls) {
      fieldSet.add(f.trim()); //Handle spaces in the field list.
    }

    String[] sorts = sort.split(",");
    StreamComparator[] comps = new StreamComparator[sorts.length];
    for(int i=0; i<sorts.length; i++) {
      String s = sorts[i];

      String[] spec = COMPILE.split(s.trim()); //This should take into account spaces in the sort spec.

      if (spec.length != 2) {
        throw new IOException("Invalid sort spec:" + s);
      }

      String fieldName = spec[0].trim();
      String order = spec[1].trim();

      if(!fieldSet.contains(spec[0])) {
        throw new IOException("Fields in the sort spec must be included in the field list:"+spec[0]);
      }

      // if there's an alias for the field then use the alias
      if(null != fieldMappings && fieldMappings.containsKey(fieldName)){
        fieldName = fieldMappings.get(fieldName);
      }

      comps[i] = new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
    }

    if(comps.length > 1) {
      return new MultipleFieldComparator(comps);
    } else {
      return comps[0];
    }
  }

  public static Slice[] getSlices(String collectionName, ZkStateReader zkStateReader, boolean checkAlias) throws IOException {

    Map<String,ClusterState.CollectionRef> collectionsMap = zkStateReader.getCollectionRefs();

    //TODO we should probably split collection by comma to query more than one
    //  which is something already supported in other parts of Solr

    // check for alias or collection

    List<String> allCollections = new ArrayList<>();
    String[] collectionNames = collectionName.split(",");
    for(String col : collectionNames) {
      List<String> collections = checkAlias
          ? zkStateReader.getAliases().resolveAliases(col)  // if not an alias, returns collectionName
          : Collections.singletonList(collectionName);
      allCollections.addAll(collections);
    }

    // Lookup all actives slices for these collections
    List<Slice> slices = allCollections.stream()
        .map(collectionsMap::get)
        .filter(Objects::nonNull)
        .flatMap(docCol -> docCol.get().join().getActiveSlices().stream())
        .collect(Collectors.toList());
    if (!slices.isEmpty()) {
      return slices.toArray(new Slice[0]);
    }

    // Check collection case insensitive
    for(Entry<String,ClusterState.CollectionRef> entry : collectionsMap.entrySet()) {
      if(entry.getKey().equalsIgnoreCase(collectionName)) {
        return entry.getValue().get().join().getActiveSlices().toArray(SLICES);
      }
    }

    throw new IOException("Slices not found for " + collectionName);
  }

  protected void constructStreams() throws IOException {
    try {


      ModifiableSolrParams mParams = new ModifiableSolrParams(params);
      mParams = adjustParams(mParams);
      mParams.set(DISTRIB, "false"); // We are the aggregator.

      List<String> shardUrls = getShards(this.zkHost, this.collection, this.streamContext, mParams);

      for(String shardUrl : shardUrls) {
        SolrStream solrStream = new SolrStream(shardUrl, mParams);
        if(streamContext != null) {
          solrStream.setStreamContext(streamContext);
          if (streamContext.isLocal()) {
            solrStream.setDistrib(false);
          }
        }
        solrStream.setFieldMappings(this.fieldMappings);
        solrStreams.add(solrStream);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  void openStreams() throws IOException {
    ExecutorService service = ParWork.getRootSharedIOExecutor();

      List<Future<TupleWrapper>> futures = new ArrayList();
      for (TupleStream solrStream : solrStreams) {
        StreamOpener so = new StreamOpener((SolrStream) solrStream, comp, eofTuples);
        Future<TupleWrapper> future = service.submit(so);
        futures.add(future);
      }

      try {
        for (Future<TupleWrapper> f : futures) {
          TupleWrapper w = f.get();
          if (w != null) {
            System.err.println("XCJF_DEBUG shard added: " + w.stream.getBaseUrl());
            tuples.add(w);
          } else {
            System.err.println("XCJF_DEBUG shard returned null (no docs or EOF)");
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      System.err.println("XCJF_DEBUG tuples.size after open=" + tuples.size() + " solrStreams.size=" + solrStreams.size());
  }

  /**
   *  Closes the CloudSolrStream
   **/
  public void close() throws IOException {
    if(solrStreams != null) {
      for (TupleStream solrStream : solrStreams) {
        solrStream.close();
      }
    }
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return comp;
  }

  public Tuple read() throws IOException {
    return _read();
  }

  protected Tuple _read() throws IOException {
    TupleWrapper tw = tuples.pollFirst();
    if(tw != null) {
      Tuple t = tw.getTuple();

      if (trace) {
        t.put("_COLLECTION_", this.collection);
      }

      if(tw.next()) {
        tuples.add(tw);
      }
      return t;
    } else {
      // Check if any shards finished with an EXCEPTION tuple and propagate it.
      for (Tuple eofTuple : eofTuples.values()) {
        if (eofTuple.EXCEPTION) {
          throw new IOException(eofTuple.getException());
        }
      }
      Tuple tuple = Tuple.EOF();
      if(trace) {
        tuple.put("_COLLECTION_", this.collection);
      }
      return tuple;
    }
  }

  protected static class TupleWrapper implements Comparable<TupleWrapper> {
    private final Map<String,Tuple> eofTuples;
    private volatile Tuple tuple;
    private final SolrStream stream;
    private final StreamComparator comp;

    public TupleWrapper(SolrStream stream, StreamComparator comp, Map<String, Tuple> eofTuples) {
      this.eofTuples = eofTuples;
      this.stream = stream;
      this.comp = comp;
    }

    public int compareTo(TupleWrapper w) {
      if(this == w) {
        return 0;
      }

      int i = comp.compare(tuple, w.tuple);
      if(i == 0) {
        return 1;
      } else {
        return i;
      }
    }

    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tuple);
    }

    public Tuple getTuple() {
      return tuple;
    }

    public boolean next() throws IOException {
      this.tuple = stream.read();

      if(tuple.EOF) {
        eofTuples.put(stream.getBaseUrl(), tuple);
        System.err.println("XCJF_DEBUG shard EOF: " + stream.getBaseUrl());
      } else {
        System.err.println("XCJF_DEBUG shard tuple from " + stream.getBaseUrl() + ": " + tuple.getMap());
      }

      return !tuple.EOF;
    }
  }

  protected static class StreamOpener implements Callable<TupleWrapper> {

    private final SolrStream stream;
    private final StreamComparator comp;
    Map<String, Tuple> eofTuples;

    public StreamOpener(SolrStream stream, StreamComparator comp, Map<String, Tuple> eofTuples) {
      this.stream = stream;
      this.comp = comp;
      this.eofTuples = eofTuples;
    }

    public TupleWrapper call() throws Exception {
      stream.open();
      TupleWrapper wrapper = new TupleWrapper(stream, comp, eofTuples);
      if(wrapper.next()) {
        return wrapper;
      } else {
        return null;
      }
    }
  }

  protected ModifiableSolrParams adjustParams(ModifiableSolrParams params) {
    return params;
  }
}

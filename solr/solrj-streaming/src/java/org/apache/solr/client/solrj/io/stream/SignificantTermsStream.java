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

import static org.apache.solr.client.solrj.io.stream.StreamExecutorHelper.submitAllAndAwaitAggregatingExceptions;
import static org.apache.solr.common.params.CommonParams.DISTRIB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * @since 6.5.0
 */
public class SignificantTermsStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected Map<String, String> params;
  protected Iterator<Tuple> tupleIterator;
  protected String field;
  protected int numTerms;
  protected float minDocFreq;
  protected float maxDocFreq;
  protected int minTermLength;

  private transient SolrClientCache clientCache;
  private transient boolean doCloseCache;
  private transient StreamContext streamContext;

  public SignificantTermsStream(
      String zkHost,
      String collectionName,
      Map<String, String> params,
      String field,
      float minDocFreq,
      float maxDocFreq,
      int minTermLength,
      int numTerms)
      throws IOException {

    init(collectionName, zkHost, params, field, minDocFreq, maxDocFreq, minTermLength, numTerms);
  }

  public SignificantTermsStream(StreamExpression expression, StreamFactory factory)
      throws IOException {
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't
    // need to count it twice
    if (expression.getParameters().size() != 1 + namedParams.size()) {
      throw new IOException(
          String.format(Locale.ROOT, "invalid expression %s - unknown operands found", expression));
    }

    // Collection Name
    if (null == collectionName) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "invalid expression %s - collectionName expected as first operand",
              expression));
    }

    // Named parameters - passed directly to solr as solrparams
    if (0 == namedParams.size()) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",
              expression));
    }

    Map<String, String> params = new HashMap<>();
    for (StreamExpressionNamedParameter namedParam : namedParams) {
      if (!namedParam.getName().equals("zkHost")) {
        params.put(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    String fieldParam = params.get("field");
    if (fieldParam != null) {
      params.remove("field");
    } else {
      throw new IOException("field param cannot be null for SignificantTermsStream");
    }

    String numTermsParam = params.get("limit");
    int numTerms = 20;
    if (numTermsParam != null) {
      numTerms = Integer.parseInt(numTermsParam);
      params.remove("limit");
    }

    String minTermLengthParam = params.get("minTermLength");
    int minTermLength = 4;
    if (minTermLengthParam != null) {
      minTermLength = Integer.parseInt(minTermLengthParam);
      params.remove("minTermLength");
    }

    String minDocFreqParam = params.get("minDocFreq");
    float minDocFreq = 5.0F;
    if (minDocFreqParam != null) {
      minDocFreq = Float.parseFloat(minDocFreqParam);
      params.remove("minDocFreq");
    }

    String maxDocFreqParam = params.get("maxDocFreq");
    float maxDocFreq = .3F;
    if (maxDocFreqParam != null) {
      maxDocFreq = Float.parseFloat(maxDocFreqParam);
      params.remove("maxDocFreq");
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if (null == zkHostExpression) {
      zkHost = factory.getCollectionZkHost(collectionName);
    } else if (zkHostExpression.getParameter() instanceof StreamExpressionValue) {
      zkHost = ((StreamExpressionValue) zkHostExpression.getParameter()).getValue();
    }

    if (zkHost == null) {
      zkHost = factory.getDefaultZkHost();
    }

    // We've got all the required items
    init(
        collectionName,
        zkHost,
        params,
        fieldParam,
        minDocFreq,
        maxDocFreq,
        minTermLength,
        numTerms);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // functionName(collectionName, param1, param2, ..., paramN, sort="comp",
    // [aliases="field=alias,..."])

    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // collection
    expression.addParameter(collection);

    // parameters
    for (Map.Entry<String, String> param : params.entrySet()) {
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), param.getValue()));
    }

    expression.addParameter(new StreamExpressionNamedParameter("field", field));
    expression.addParameter(
        new StreamExpressionNamedParameter("minDocFreq", Float.toString(minDocFreq)));
    expression.addParameter(
        new StreamExpressionNamedParameter("maxDocFreq", Float.toString(maxDocFreq)));
    expression.addParameter(
        new StreamExpressionNamedParameter("numTerms", String.valueOf(numTerms)));
    expression.addParameter(
        new StreamExpressionNamedParameter("minTermLength", String.valueOf(minTermLength)));

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    return expression;
  }

  private void init(
      String collectionName,
      String zkHost,
      Map<String, String> params,
      String field,
      float minDocFreq,
      float maxDocFreq,
      int minTermLength,
      int numTerms)
      throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = params;
    this.field = field;
    this.minDocFreq = minDocFreq;
    this.maxDocFreq = maxDocFreq;
    this.numTerms = numTerms;
    this.minTermLength = minTermLength;
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.clientCache = context.getSolrClientCache();
    this.streamContext = context;
  }

  @Override
  public void open() throws IOException {
    if (clientCache == null) {
      doCloseCache = true;
      clientCache = new SolrClientCache();
    } else {
      doCloseCache = false;
    }
  }

  @Override
  public List<TupleStream> children() {
    return null;
  }

  private Collection<NamedList<?>> callShards(List<String> baseUrls) throws IOException {
    List<SignificantTermsCall> tasks = new ArrayList<>();
    for (String baseUrl : baseUrls) {
      SignificantTermsCall lc =
          new SignificantTermsCall(
              baseUrl,
              this.params,
              this.field,
              this.minDocFreq,
              this.maxDocFreq,
              this.minTermLength,
              this.numTerms,
              streamContext.isLocal(),
              clientCache);
      tasks.add(lc);
    }
    return submitAllAndAwaitAggregatingExceptions(tasks, "SignificantTermsStream");
  }

  @Override
  public void close() throws IOException {
    if (doCloseCache) {
      clientCache.close();
    }
  }

  /** Return the stream sort - ie, the order in which records are returned */
  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory).toString());
  }

  @Override
  public Tuple read() throws IOException {
    try {
      if (tupleIterator == null) {
        Map<String, int[]> mergeFreqs = new HashMap<>();
        long numDocs = 0;
        long resultCount = 0;
        for (NamedList<?> fullResp : callShards(getShards(zkHost, collection, streamContext))) {
          Map<?, ?> stResp = (Map<?, ?>) fullResp.get("significantTerms");

          @SuppressWarnings({"unchecked"})
          List<String> terms = (List<String>) stResp.get("sterms");
          @SuppressWarnings({"unchecked"})
          List<Integer> docFreqs = (List<Integer>) stResp.get("docFreq");
          @SuppressWarnings({"unchecked"})
          List<Integer> queryDocFreqs = (List<Integer>) stResp.get("queryDocFreq");
          numDocs += (Integer) stResp.get("numDocs");

          SolrDocumentList searchResp = (SolrDocumentList) fullResp.get("response");
          resultCount += searchResp.getNumFound();

          for (int i = 0; i < terms.size(); i++) {
            String term = terms.get(i);
            int docFreq = docFreqs.get(i);
            int queryDocFreq = queryDocFreqs.get(i);
            if (!mergeFreqs.containsKey(term)) {
              mergeFreqs.put(term, new int[2]);
            }

            int[] freqs = mergeFreqs.get(term);
            freqs[0] += docFreq;
            freqs[1] += queryDocFreq;
          }
        }

        List<Map<String, Object>> maps = new ArrayList<>();

        for (Map.Entry<String, int[]> entry : mergeFreqs.entrySet()) {
          int[] freqs = entry.getValue();
          Map<String, Object> map = new HashMap<>();
          map.put("term", entry.getKey());
          map.put("background", freqs[0]);
          map.put("foreground", freqs[1]);

          float score =
              (float) (Math.log(freqs[1]) + 1.0)
                  * (float) (Math.log(((float) (numDocs + 1)) / (freqs[0] + 1)) + 1.0);

          map.put("score", score);
          maps.add(map);
        }

        maps.sort(new ScoreComp());
        List<Tuple> tuples = new ArrayList<>();
        for (Map<String, Object> map : maps) {
          if (tuples.size() == numTerms) break;
          tuples.add(new Tuple(map));
        }

        tuples.add(Tuple.EOF());
        tupleIterator = tuples.iterator();
      }

      return tupleIterator.next();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static class ScoreComp implements Comparator<Map<String, ?>> {
    @Override
    public int compare(Map<String, ?> a, Map<String, ?> b) {
      Float scorea = (Float) a.get("score");
      Float scoreb = (Float) b.get("score");
      return scoreb.compareTo(scorea);
    }
  }

  protected static class SignificantTermsCall implements Callable<NamedList<?>> {

    private final String baseUrl;
    private final String field;
    private final float minDocFreq;
    private final float maxDocFreq;
    private final int numTerms;
    private final int minTermLength;
    private final Map<String, String> paramsMap;
    private final boolean isLocal;
    private final SolrClientCache clientCache;

    public SignificantTermsCall(
        String baseUrl,
        Map<String, String> paramsMap,
        String field,
        float minDocFreq,
        float maxDocFreq,
        int minTermLength,
        int numTerms,
        boolean isLocal,
        SolrClientCache clientCache) {

      this.baseUrl = baseUrl;
      this.field = field;
      this.minDocFreq = minDocFreq;
      this.maxDocFreq = maxDocFreq;
      this.paramsMap = paramsMap;
      this.numTerms = numTerms;
      this.minTermLength = minTermLength;
      this.isLocal = isLocal;
      this.clientCache = clientCache;
    }

    @Override
    public NamedList<?> call() throws Exception {
      ModifiableSolrParams params = new ModifiableSolrParams();
      SolrClient solrClient = clientCache.getHttpSolrClient(baseUrl);

      params.add(DISTRIB, "false");
      params.add("fq", "{!significantTerms}");

      for (Map.Entry<String, String> entry : paramsMap.entrySet()) {
        params.add(entry.getKey(), entry.getValue());
      }

      params.add("minDocFreq", Float.toString(minDocFreq));
      params.add("maxDocFreq", Float.toString(maxDocFreq));
      params.add("minTermLength", Integer.toString(minTermLength));
      params.add("field", field);
      params.add("numTerms", String.valueOf(numTerms * 5));
      if (isLocal) {
        params.add("distrib", "false");
      }

      QueryRequest request = new QueryRequest(params, SolrRequest.METHOD.POST);
      QueryResponse response = request.process(solrClient);
      NamedList<?> res = response.getResponse();
      return res;
    }
  }
}

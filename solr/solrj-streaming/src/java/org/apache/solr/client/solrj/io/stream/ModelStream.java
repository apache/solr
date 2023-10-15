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

import static org.apache.solr.common.params.CommonParams.ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.client.solrj.io.ModelCache;
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

/**
 * The ModelStream retrieves a stored model from a Solr Cloud collection.
 *
 * <p>Syntax: model(collection, id="modelID")
 *
 * @since 6.3.0
 */
public class ModelStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected String modelID;
  protected ModelCache modelCache;
  protected Tuple model;
  protected long cacheMillis;

  public ModelStream(String zkHost, String collectionName, String modelID, long cacheMillis)
      throws IOException {

    init(collectionName, zkHost, modelID, cacheMillis);
  }

  public ModelStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

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

    String modelID = params.get(ID);
    if (modelID == null) {
      throw new IOException("id param cannot be null for ModelStream");
    }

    long cacheMillis = 300000;
    String cacheMillisParam = params.get("cacheMillis");

    if (cacheMillisParam != null) {
      cacheMillis = Long.parseLong(cacheMillisParam);
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

    if (null == zkHost) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "invalid expression %s - zkHost not found for collection '%s'",
              expression,
              collectionName));
    }

    // We've got all the required items
    init(collectionName, zkHost, modelID, cacheMillis);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams)
      throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    // collection
    expression.addParameter(collection);

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    expression.addParameter(new StreamExpressionNamedParameter(ID, modelID));
    expression.addParameter(
        new StreamExpressionNamedParameter("cacheMillis", Long.toString(cacheMillis)));

    return expression;
  }

  private void init(String collectionName, String zkHost, String modelID, long cacheMillis)
      throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.modelID = modelID;
    this.cacheMillis = cacheMillis;
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.modelCache = context.getModelCache();
  }

  @Override
  public void open() throws IOException {
    this.model = modelCache.getModel(collection, modelID, cacheMillis);
  }

  @Override
  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<>();
    return l;
  }

  @Override
  public void close() throws IOException {}

  /** Return the stream sort - ie, the order in which records are returned */
  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(Explanation.ExpressionType.MACHINE_LEARNING_MODEL);
    explanation.setExpression(toExpression(factory).toString());

    return explanation;
  }

  @Override
  public Tuple read() throws IOException {
    Tuple tuple = null;

    if (model != null) {
      tuple = model;
      model = null;
    } else {
      tuple = Tuple.EOF();
    }

    return tuple;
  }
}

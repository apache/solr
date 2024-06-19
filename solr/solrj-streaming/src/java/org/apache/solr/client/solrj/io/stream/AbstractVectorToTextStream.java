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
import java.util.List;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.DefaultStreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class AbstractVectorToTextStream extends TupleStream implements Expressible {

  private static final String VECTOR_FIELD_PARAM = "vectorField";
  private static final String OUTPUT_KEY_PARAM = "outputKey";

  private final TupleStream tupleStream;

  private final String vectorField;
  private final String outputKey;

  public AbstractVectorToTextStream(StreamExpression streamExpression, StreamFactory streamFactory)
      throws IOException {

    final List<StreamExpression> streamExpressions =
        streamFactory.getExpressionOperandsRepresentingTypes(
            streamExpression, Expressible.class, TupleStream.class);
    if (streamExpressions.size() == 1) {
      this.tupleStream = streamFactory.constructStream(streamExpressions.get(0));
    } else {
      throw new IOException("Expected exactly one stream in expression: " + streamExpression);
    }

    this.vectorField = getOperandValue(streamExpression, streamFactory, VECTOR_FIELD_PARAM);
    this.outputKey = getOperandValue(streamExpression, streamFactory, OUTPUT_KEY_PARAM);

    if (!(streamFactory instanceof DefaultStreamFactory)) {
      throw new IOException(
          this.getClass().getName()
              + " requires a "
              + DefaultStreamFactory.class.getName()
              + " StreamFactory");
    }
  }

  protected static String getOperandValue(
      StreamExpression streamExpression, StreamFactory streamFactory, String operandName)
      throws IOException {
    final StreamExpressionNamedParameter namedParameter =
        streamFactory.getNamedOperand(streamExpression, operandName);
    String operandValue = null;
    if (namedParameter != null && namedParameter.getParameter() instanceof StreamExpressionValue) {
      operandValue = ((StreamExpressionValue) namedParameter.getParameter()).getValue();
    }
    if (operandValue == null) {
      throw new IOException("Expected '" + operandName + "' in expression: " + streamExpression);
    } else {
      return operandValue;
    }
  }

  public void setStreamContext(StreamContext streamContext) {
    tupleStream.setStreamContext(streamContext);
  }

  public List<TupleStream> children() {
    return tupleStream.children();
  }

  public void open() throws IOException {
    tupleStream.open();
  }

  public void close() throws IOException {
    tupleStream.close();
  }

  public Tuple read() throws IOException {
    List<List<Double>> vectors = new ArrayList<>();
    Tuple tuple = tupleStream.read();
    while (!tuple.EOF) {
      vectors.add(tuple.getDoubles(this.vectorField));
      tuple = tupleStream.read();
    }

    final Tuple result = new Tuple();
    result.put(this.outputKey, vectorToText(vectors));
    result.EOF = true;
    return result;
  }

  protected abstract String vectorToText(List<List<Double>> vectors);

  public StreamComparator getStreamSort() {
    return tupleStream.getStreamSort();
  }

  public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[] {tupleStream.toExplanation(streamFactory)})
        .withExpressionType(ExpressionType.STREAM_DECORATOR)
        .withFunctionName(streamFactory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpression(toExpression(streamFactory, false).toString());
  }

  public StreamExpressionParameter toExpression(StreamFactory streamFactory) throws IOException {
    return toExpression(streamFactory, true /* includeStreams */);
  }

  protected StreamExpression toExpression(StreamFactory streamFactory, boolean includeStreams)
      throws IOException {
    final String functionName = streamFactory.getFunctionName(this.getClass());
    final StreamExpression streamExpression = new StreamExpression(functionName);

    if (includeStreams) {
      if (this.tupleStream instanceof Expressible) {
        streamExpression.addParameter(((Expressible) this.tupleStream).toExpression(streamFactory));
      } else {
        throw new IOException(
            "This "
                + this.getClass().getName()
                + " contains a non-Expressible TupleStream "
                + this.tupleStream.getClass().getName());
      }
    } else {
      streamExpression.addParameter("<stream>");
    }

    streamExpression.addParameter(
        new StreamExpressionNamedParameter(VECTOR_FIELD_PARAM, this.vectorField));
    streamExpression.addParameter(
        new StreamExpressionNamedParameter(OUTPUT_KEY_PARAM, this.outputKey));

    return streamExpression;
  }
}

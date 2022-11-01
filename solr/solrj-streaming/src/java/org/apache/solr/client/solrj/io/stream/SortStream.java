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
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * The SortStream emits a stream of Tuples sorted by a Comparator.
 *
 * @since 6.1.0
 */
public class SortStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private StreamComparator comparator;
  private Worker worker;

  public SortStream(TupleStream stream, StreamComparator comp) throws IOException {
    init(stream, comp);
  }

  public SortStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions =
        factory.getExpressionOperandsRepresentingTypes(
            expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter byExpression = factory.getNamedOperand(expression, "by");

    // validate expression contains only what we want.
    if (expression.getParameters().size() != streamExpressions.size() + 1) {
      throw new IOException(
          String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));
    }

    if (1 != streamExpressions.size()) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expecting a single stream but found %d",
              expression,
              streamExpressions.size()));
    }

    if (null == byExpression || !(byExpression.getParameter() instanceof StreamExpressionValue)) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expecting single 'by' parameter listing fields to sort over but didn't find one",
              expression));
    }

    init(
        factory.constructStream(streamExpressions.get(0)),
        factory.constructComparator(
            ((StreamExpressionValue) byExpression.getParameter()).getValue(),
            FieldComparator.class));
  }

  private void init(TupleStream stream, StreamComparator comp) throws IOException {
    this.stream = stream;
    this.comparator = comp;

    // standard java modified merge sort
    worker =
        new Worker() {

          @SuppressWarnings("JdkObsolete")
          private final LinkedList<Tuple> tuples = new LinkedList<>();

          private Tuple eofTuple;

          @Override
          public void readStream(TupleStream stream) throws IOException {
            Tuple tuple = stream.read();
            while (!tuple.EOF) {
              tuples.add(tuple);
              tuple = stream.read();
            }
            eofTuple = tuple;
          }

          @Override
          public void sort() {
            tuples.sort(comparator);
          }

          @Override
          public Tuple read() {
            if (tuples.isEmpty()) {
              return eofTuple;
            }
            return tuples.removeFirst();
          }
        };
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams)
      throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    if (includeStreams) {
      // streams
      if (stream instanceof Expressible) {
        expression.addParameter(((Expressible) stream).toExpression(factory));
      } else {
        throw new IOException(
            "This SortStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    } else {
      expression.addParameter("<stream>");
    }

    // by
    if (comparator != null) {
      expression.addParameter(
          new StreamExpressionNamedParameter("by", comparator.toExpression(factory)));
    } else {
      throw new IOException(
          "This SortStream contains a non-expressible equalitor - it cannot be converted to an expression");
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[] {stream.toExplanation(factory)})
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory, false).toString())
        .withHelper(comparator.toExplanation(factory));
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }

  @Override
  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<>();
    l.add(stream);
    return l;
  }

  @Override
  public void open() throws IOException {
    stream.open();

    worker.readStream(stream);
    worker.sort();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public Tuple read() throws IOException {
    // return next from sorted order
    return worker.read();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  @Override
  public StreamComparator getStreamSort() {
    return comparator;
  }

  @Override
  public int getCost() {
    return 0;
  }

  private interface Worker {
    public void readStream(TupleStream stream) throws IOException;

    public void sort();

    public Tuple read();
  }
}

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

import static org.apache.solr.common.params.CommonParams.SORT;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.RankStream.ReverseComp;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  Iterates over a TupleStream and Ranks the top N tuples for specified start param based on a Comparator.
 *
 *  @since 9.0.0
 *
 */

public class PagingStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private StreamComparator comp;
  private int start;
  private int rows;
  private transient PriorityQueue<Tuple> top;
  private transient boolean finished = false;
  private transient Deque<Tuple> topList;

  public PagingStream(TupleStream tupleStream, int start, int rows, StreamComparator comp)
      throws IOException {
    init(tupleStream, start, rows, comp);
  }

  public PagingStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions =
        factory.getExpressionOperandsRepresentingTypes(
            expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter startParam = factory.getNamedOperand(expression, "start");
    StreamExpressionNamedParameter rowsParam = factory.getNamedOperand(expression, "rows");
    StreamExpressionNamedParameter sortExpression = factory.getNamedOperand(expression, SORT);

    // validate expression contains only what we want.
    if (expression.getParameters().size() != streamExpressions.size() + 3) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - unknown operands found: Expression Parameter Size: %s, StreamExpression Parameter Size: %s",
              expression,
              expression.getParameters().size(),
              streamExpressions.size()));
    }

    if (null == startParam
        || null == startParam.getParameter()
        || !(startParam.getParameter() instanceof StreamExpressionValue)) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expecting a single 'start' parameter of type positive integer but didn't find one",
              expression));
    }

    if (null == rowsParam
        || null == rowsParam.getParameter()
        || !(rowsParam.getParameter() instanceof StreamExpressionValue)) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expecting a single 'rows' parameter of type positive integer but didn't find one",
              expression));
    }

    String startStr = ((StreamExpressionValue) startParam.getParameter()).getValue();
    String rowsStr = ((StreamExpressionValue) rowsParam.getParameter()).getValue();

    int startInt = 0;
    try {
      startInt = Integer.parseInt(startStr);
      if (startInt < 0) {
        throw new IOException(
            String.format(
                Locale.ROOT,
                "invalid expression %s - topN '%s' must not be less than 0.",
                expression,
                startStr));
      }
    } catch (NumberFormatException e) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "invalid expression %s - topN '%s' is not a valid integer.",
              expression,
              startStr));
    }

    int rowsInt = 0;
    try {
      rowsInt = Integer.parseInt(rowsStr);
      if (rowsInt <= 0) {
        throw new IOException(
            String.format(
                Locale.ROOT,
                "invalid expression %s - topN '%s' must be greater than 0.",
                expression,
                rowsStr));
      }
    } catch (NumberFormatException e) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "invalid expression %s - topN '%s' is not a valid integer.",
              expression,
              rowsStr));
    }

    if (1 != streamExpressions.size()) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expecting a single stream but found %d",
              expression,
              streamExpressions.size()));
    }
    if (null == sortExpression
        || !(sortExpression.getParameter() instanceof StreamExpressionValue)) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expecting single 'over' parameter listing fields to unique over but didn't find one",
              expression));
    }

    TupleStream stream = factory.constructStream(streamExpressions.get(0));
    StreamComparator comp =
        factory.constructComparator(
            ((StreamExpressionValue) sortExpression.getParameter()).getValue(),
            FieldComparator.class);

    init(stream, startInt, rowsInt, comp);
  }

  private void init(TupleStream tupleStream, int start, int rows, StreamComparator comp)
      throws IOException {
    this.stream = tupleStream;
    this.comp = comp;
    this.start = start;
    this.rows = rows;

    // Paging stream does not demand that its order is derivable from the order of the incoming
    // stream. No derivation check required
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {

    return toExpression(factory, true);
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
    this.top = new PriorityQueue<>(rows, new ReverseComp(comp));
    this.topList = new ArrayDeque<>();
    stream.open();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public Tuple read() throws IOException {

    /*   1. Read the stream and add N (rows+start) tuples into priority Queue.
     *   2. If new tuple from stream is greater than Tuple in 'top' priority Queue, replace tuple in priority Queue by new tuple.
     *   3. Add required (specified by rows param) in to 'topList' Queue.
     */
    if (!finished) {
      while (true) {
        Tuple tuple = stream.read();
        if (tuple.EOF) { // 3. Add required (specified by rows param) in to 'topList' Queue.
          finished = true;
          int s = top.size();
          for (int i = 0; i < rows; i++) {
            Tuple t = top.poll();
            topList.addFirst(t);
          }
          topList.addLast(tuple);
          break;
        } else {
          if (top.size()
              >= (rows
                  + start)) { // 2. If new tuple from stream is greater than Tuple in 'top' priority
            // Queue, replace tuple in priority Queue by new tuple.
            Tuple peek = top.peek();
            if (comp.compare(tuple, peek) < 0) {
              top.poll();
              top.add(tuple);
            }
          } else { // 1. Read the stream and add N (rows+start) tuples into priority Queue.
            top.add(tuple);
          }
        }
      }
    }
    return topList.pollFirst();
  }

  @Override
  public StreamComparator getStreamSort() {
    return this.comp;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[] {stream.toExplanation(factory)})
        .withFunctionName(factory.getFunctionName(this.getClass()))
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(ExpressionType.STREAM_DECORATOR)
        .withExpression(toExpression(factory, false).toString())
        .withHelper(comp.toExplanation(factory));
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams)
      throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // start
    expression.addParameter(new StreamExpressionNamedParameter("start", Integer.toString(start)));
    expression.addParameter(new StreamExpressionNamedParameter("rows", Integer.toString(rows)));

    if (includeStreams) {
      // stream
      if (stream instanceof Expressible) {
        expression.addParameter(((Expressible) stream).toExpression(factory));
      } else {
        throw new IOException(
            "This PagingStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    } else {
      expression.addParameter("<stream>");
    }

    // sort
    expression.addParameter(new StreamExpressionNamedParameter(SORT, comp.toExpression(factory)));

    return expression;
  }
}

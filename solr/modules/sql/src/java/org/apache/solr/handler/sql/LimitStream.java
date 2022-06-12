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
package org.apache.solr.handler.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;


public class LimitStream extends TupleStream implements Expressible {

  private final TupleStream stream;
  private final int limit;
  private final int offset;
  private int count;

  public LimitStream(StreamExpression expression, StreamFactory factory) throws IOException {

    List<StreamExpression> streamExpressions =
        factory.getExpressionOperandsRepresentingTypes(
            expression, Expressible.class, TupleStream.class);
    this.stream = factory.constructStream(streamExpressions.get(0));

    StreamExpressionNamedParameter limitExpression = factory.getNamedOperand(expression, "limit");

    if (limitExpression == null) {
      throw new IOException("Invalid expression limit parameter expected");
    }

    this.limit =
        Integer.parseInt(((StreamExpressionValue) limitExpression.getParameter()).getValue());

    StreamExpressionNamedParameter offsetExpression = factory.getNamedOperand(expression, "offset");
    if (offsetExpression == null) {
      this.offset = 0;
    } else {
      this.offset =
          Integer.parseInt(((StreamExpressionValue) offsetExpression.getParameter()).getValue());
    }
  }

  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression("limit");

    if (stream instanceof Expressible) {
      expression.addParameter(((Expressible) stream).toExpression(factory));
    } else {
      throw new IOException(
          "This Stream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }

    expression.addParameter(
        new StreamExpressionNamedParameter("limit", Integer.toString(this.limit)));

    expression.addParameter(
        new StreamExpressionNamedParameter("offset", Integer.toString(this.offset)));
    return expression;
  }

  LimitStream(TupleStream stream, int limit) {
    this(stream, limit, 0);
  }

  LimitStream(TupleStream stream, int limit, int offset) {
    this.stream = stream;
    this.limit = limit;
    this.offset = offset > 0 ? offset : 0;
    this.count = 0;
  }

  public void open() throws IOException {
    this.stream.open();
  }

  public void close() throws IOException {
    this.stream.close();
  }

  public List<TupleStream> children() {
    List<TupleStream> children = new ArrayList<>();
    children.add(stream);
    return children;
  }

  public StreamComparator getStreamSort() {
    return stream.getStreamSort();
  }

  public void setStreamContext(StreamContext context) {
    stream.setStreamContext(context);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[] {stream.toExplanation(factory)})
        .withFunctionName("SQL LIMIT")
        .withExpression("--non-expressible--")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR);
  }

  public Tuple read() throws IOException {

    if (count == 0 && offset > 0) {
      // skip offset # of sorted tuples (indexes 0 to offset-1) so that the first tuple returned
      while (count < offset) {
        ++count; // don't increment until after the compare ...
        Tuple skip = stream.read();
        if (skip.EOF) {
          return skip;
        }
      }
    }

    // done once we've reached the tuple after limit + offset
    if (++count > (limit + offset)) {
      return Tuple.EOF();
    }

    return stream.read();
  }
}

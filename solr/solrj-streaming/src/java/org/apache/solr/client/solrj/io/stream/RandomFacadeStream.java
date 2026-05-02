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

import static org.apache.solr.common.params.CommonParams.ROWS;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.ModifiableSolrParams;

public class RandomFacadeStream extends TupleStream implements Expressible {

  private TupleStream innerStream;

  public RandomFacadeStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    // Collection Name
    if (null == collectionName) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "invalid expression %s - collectionName expected as first operand",
              expression));
    }

    // pull out known named params
    ModifiableSolrParams params =
        buildSolrParamsExcept(
            namedParams, "solrConnection", "zkHost", "buckets", "bucketSorts", "limit");

    // Add sensible defaults

    if (params.get("q") == null) {
      params.add("q", "*:*");
    }

    if (params.get("fl") == null) {
      params.add("fl", "*");
    }

    if (params.get("rows") == null) {
      params.add("rows", "500");
    }

    var solrConnection = buildSolrConnection(factory, expression, collectionName);

    if (params.get(ROWS) != null) {
      int rows = Integer.parseInt(params.get(ROWS));
      if (rows >= 5000) {
        DeepRandomStream deepRandomStream = new DeepRandomStream();
        deepRandomStream.init(solrConnection, collectionName, params);
        this.innerStream = deepRandomStream;
      } else {
        RandomStream randomStream = new RandomStream();
        randomStream.init(solrConnection, collectionName, params);
        this.innerStream = randomStream;
      }
    } else {
      RandomStream randomStream = new RandomStream();
      randomStream.init(solrConnection, collectionName, params);
      this.innerStream = randomStream;
    }
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return ((Expressible) innerStream).toExpression(factory);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return innerStream.toExplanation(factory);
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.innerStream.setStreamContext(context);
  }

  @Override
  public List<TupleStream> children() {
    return innerStream.children();
  }

  @Override
  public void open() throws IOException {
    innerStream.open();
  }

  @Override
  public void close() throws IOException {
    innerStream.close();
  }

  @Override
  public Tuple read() throws IOException {
    return innerStream.read();
  }

  @Override
  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }
}

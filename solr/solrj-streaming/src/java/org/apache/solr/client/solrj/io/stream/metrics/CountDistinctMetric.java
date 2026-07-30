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
package org.apache.solr.client.solrj.io.stream.metrics;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CountDistinctMetric extends Metric {

  public static final String COUNT_DISTINCT = "countDist";
  public static final String APPROX_COUNT_DISTINCT = "hll";

  private String columnName;
  private HashSet<Object> distinctValues = new HashSet<>();

  public CountDistinctMetric(String columnName) {
    this(columnName, false);
  }

  public CountDistinctMetric(String columnName, boolean isApproximate) {
    init(isApproximate ? APPROX_COUNT_DISTINCT : COUNT_DISTINCT, columnName);
  }

  public CountDistinctMetric(StreamExpression expression, StreamFactory factory)
      throws IOException {
    // grab all parameters out
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);

    // validate expression contains only what we want.
    if (null == columnName) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expected %s(columnName)",
              expression,
              functionName));
    }
    if (1 != expression.getParameters().size()) {
      throw new IOException(
          String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));
    }

    init(functionName, columnName);
  }

  private void init(String functionName, String columnName) {
    this.columnName = columnName;
    this.outputLong = true;
    setFunctionName(functionName);
    setIdentifier(functionName, "(", columnName, ")");
  }

  @Override
  public void update(Tuple tuple) {
    Object value = tuple.get(columnName);
    if (value != null) {
      distinctValues.add(value);
    }
  }

  @Override
  public Metric newInstance() {
    return new CountDistinctMetric(columnName);
  }

  @Override
  public String[] getColumns() {
    return new String[] {columnName};
  }

  @Override
  public Number getValue() {
    return distinctValues.size();
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName);
  }
}

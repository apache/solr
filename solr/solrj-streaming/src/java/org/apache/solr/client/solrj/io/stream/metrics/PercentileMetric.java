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

import com.tdunning.math.stats.AVLTreeDigest;
import java.io.IOException;
import java.util.Locale;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class PercentileMetric extends Metric {

  private static final int COMPRESSION = 100;

  private String columnName;
  private double percentile;
  private AVLTreeDigest digest;

  public PercentileMetric(String columnName, double percentile) {
    init("per", columnName, percentile);
  }

  public PercentileMetric(StreamExpression expression, StreamFactory factory) throws IOException {
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);
    String percentileStr = factory.getValueOperand(expression, 1);

    if (null == columnName) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expected %s(columnName, percentile)",
              expression,
              functionName));
    }
    if (2 != expression.getParameters().size()) {
      throw new IOException(
          String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));
    }

    double percentile = Double.parseDouble(percentileStr);
    init(functionName, columnName, percentile);
  }

  private void init(String functionName, String columnName, double percentile) {
    if (percentile < 0 || percentile > 100) {
      throw new IllegalArgumentException("percentile must be between 0 and 100, got " + percentile);
    }
    this.columnName = columnName;
    this.percentile = percentile;
    this.digest = new AVLTreeDigest(COMPRESSION);
    setFunctionName(functionName);
    setIdentifier(functionName, "(", columnName, "," + formatPercentile(percentile), ")");
  }

  private static String formatPercentile(double percentile) {
    if (percentile == Math.floor(percentile) && !Double.isInfinite(percentile)) {
      return Integer.toString((int) percentile);
    }
    return String.valueOf(percentile);
  }

  @Override
  public void update(Tuple tuple) {
    Object o = tuple.get(columnName);
    if (o instanceof Double) {
      Double d = (Double) o;
      digest.add(d);
    } else if (o instanceof Float) {
      Float f = (Float) o;
      digest.add(f.doubleValue());
    } else if (o instanceof Integer) {
      Integer i = (Integer) o;
      digest.add(i.doubleValue());
    } else if (o instanceof Long) {
      Long l = (Long) o;
      digest.add(l.doubleValue());
    }
  }

  @Override
  public Number getValue() {
    if (digest.size() == 0) {
      return null;
    }
    return digest.quantile(percentile / 100.0);
  }

  @Override
  public Metric newInstance() {
    return new PercentileMetric(columnName, percentile);
  }

  @Override
  public String[] getColumns() {
    return new String[] {columnName};
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName())
        .withParameter(columnName)
        .withParameter(formatPercentile(percentile));
  }
}

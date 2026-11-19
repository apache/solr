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
import java.util.List;
import java.util.Locale;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Illustrative expression snippet:
 *
 * <pre>
 * demoVectorToText(search(myCollection,
 *                         q="*:*",
 *                         fl="id,fieldX",
 *                         sort="id asc",
 *                         qt="/export"),
 *                  vectorField="fieldX",
 *                  outputKey="answer",
 *                  demoParam="foobar")
 * </pre>
 */
public class DemoVectorToTextStream extends AbstractVectorToTextStream {

  private static final String DEMO_PARAM = "demoParam";

  private final String demoParam;

  public DemoVectorToTextStream(StreamExpression streamExpression, StreamFactory streamFactory)
      throws IOException {
    super(streamExpression, streamFactory);
    this.demoParam = getOperandValue(streamExpression, streamFactory, DEMO_PARAM);
  }

  /** TODO: replace this dummy text with something model based */
  protected String vectorToText(List<List<Double>> vectors) {
    final StringBuilder sb = new StringBuilder();
    sb.append(this.demoParam);
    sb.append("([\n");
    for (List<Double> vector : vectors) {
      sb.append(String.format(Locale.ROOT, "%s\n", vector));
    }
    sb.append("])");
    return sb.toString();
  }

  protected StreamExpression toExpression(StreamFactory streamFactory, boolean includeStreams)
      throws IOException {
    final StreamExpression streamExpression = super.toExpression(streamFactory, includeStreams);

    streamExpression.addParameter(new StreamExpressionNamedParameter(DEMO_PARAM, this.demoParam));

    return streamExpression;
  }
}

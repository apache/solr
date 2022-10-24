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
package org.apache.solr.client.solrj.io.stream.eval;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.ArrayEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

public class ArrayEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;

  public ArrayEvaluatorTest() {
    super();

    factory = new StreamFactory().withFunctionName("array", ArrayEvaluator.class);
    values = new HashMap<>();
  }

  @Test
  public void arrayLongSortAscTest() throws IOException {
    StreamEvaluator evaluator = factory.constructEvaluator("array(a,b,c, sort=asc)");
    StreamContext context = new StreamContext();
    evaluator.setStreamContext(context);

    values.put("a", 1L);
    values.put("b", 3L);
    values.put("c", 2L);

    Object tempResult = evaluator.evaluate(new Tuple(values));
    assertTrue(tempResult instanceof List<?>);

    List<?> result = (List<?>) tempResult;
    assertEquals(3, result.size());
    assertEquals(1D, result.get(0));
    assertEquals(2D, result.get(1));
    assertEquals(3D, result.get(2));
  }

  @Test
  public void arrayLongSortDescTest() throws IOException {
    StreamEvaluator evaluator = factory.constructEvaluator("array(a,b,c, sort=desc)");
    StreamContext context = new StreamContext();
    evaluator.setStreamContext(context);

    values.put("a", 1L);
    values.put("b", 3L);
    values.put("c", 2L);

    Object tempResult = evaluator.evaluate(new Tuple(values));
    assertTrue(tempResult instanceof List<?>);

    List<?> result = (List<?>) tempResult;
    assertEquals(3, result.size());
    assertEquals(3D, result.get(0));
    assertEquals(2D, result.get(1));
    assertEquals(1D, result.get(2));
  }

  @Test
  public void arrayStringSortAscTest() throws IOException {
    StreamEvaluator evaluator = factory.constructEvaluator("array(a,b,c, sort=asc)");
    StreamContext context = new StreamContext();
    evaluator.setStreamContext(context);

    values.put("a", "a");
    values.put("b", "c");
    values.put("c", "b");

    Object tempResult = evaluator.evaluate(new Tuple(values));
    assertTrue(tempResult instanceof List<?>);

    List<?> result = (List<?>) tempResult;
    assertEquals(3, result.size());
    assertEquals("a", result.get(0));
    assertEquals("b", result.get(1));
    assertEquals("c", result.get(2));
  }

  @Test
  public void arrayStringSortDescTest() throws IOException {
    StreamEvaluator evaluator = factory.constructEvaluator("array(a,b,c, sort=desc)");
    StreamContext context = new StreamContext();
    evaluator.setStreamContext(context);

    values.put("a", "a");
    values.put("b", "c");
    values.put("c", "b");

    Object tempResult = evaluator.evaluate(new Tuple(values));
    assertTrue(tempResult instanceof List<?>);

    List<?> result = (List<?>) tempResult;
    assertEquals(3, result.size());
    assertEquals("c", result.get(0));
    assertEquals("b", result.get(1));
    assertEquals("a", result.get(2));
  }

  @Test
  public void arrayStringUnsortedTest() throws IOException {
    StreamEvaluator evaluator = factory.constructEvaluator("array(a,b,c)");
    StreamContext context = new StreamContext();
    evaluator.setStreamContext(context);

    values.put("a", "a");
    values.put("b", "c");
    values.put("c", "b");

    Object tempResult = evaluator.evaluate(new Tuple(values));
    assertTrue(tempResult instanceof List<?>);

    List<?> result = (List<?>) tempResult;
    assertEquals(3, result.size());
    assertEquals("a", result.get(0));
    assertEquals("c", result.get(1));
    assertEquals("b", result.get(2));
  }
}

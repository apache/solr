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
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.OrEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

public class OrEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;

  public OrEvaluatorTest() {
    super();

    factory = new StreamFactory().withFunctionName("or", OrEvaluator.class);
    values = new HashMap<>();
  }

  @Test
  public void orTwoBooleans() throws Exception {
    StreamEvaluator evaluator = factory.constructEvaluator("or(a,b)");
    Object result;

    values.clear();
    values.put("a", true);
    values.put("b", true);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", true);
    values.put("b", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", false);
    values.put("b", true);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", false);
    values.put("b", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(false, result);
  }

  @Test
  public void orThreeBooleans() throws Exception {
    StreamEvaluator evaluator = factory.constructEvaluator("or(a,b,c)");
    Object result;

    values.clear();
    values.put("a", false);
    values.put("b", false);
    values.put("c", true);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", false);
    values.put("b", true);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", true);
    values.put("b", false);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", false);
    values.put("b", false);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(false, result);
  }

  @Test
  public void orFourBooleans() throws Exception {
    StreamEvaluator evaluator = factory.constructEvaluator("or(a,b,c,d)");

    for (boolean[] row :
        new boolean[][] {
          {true, true, true, true},
          {false, false, false, true},
          {false, true, false, false},
          {false, false, false, false}
        }) {
      values.clear();
      values.put("a", row[0]);
      values.put("b", row[1]);
      values.put("c", row[2]);
      values.put("d", row[3]);
      Object result = evaluator.evaluate(new Tuple(values));
      boolean expected = row[0] || row[1] || row[2] || row[3];
      assertEquals(expected, result);
    }
  }

  @Test
  public void orNestedWithManyArguments() throws Exception {
    StreamEvaluator evaluator = factory.constructEvaluator("or(a,or(b,c),d)");

    values.clear();
    values.put("a", false);
    values.put("b", false);
    values.put("c", false);
    values.put("d", true);
    assertEquals(true, evaluator.evaluate(new Tuple(values)));

    values.put("c", true);
    values.put("d", false);
    assertEquals(true, evaluator.evaluate(new Tuple(values)));

    values.put("c", false);
    assertEquals(false, evaluator.evaluate(new Tuple(values)));
  }

  @Test
  public void orThreeValuesRejectsNullOrNonBooleanInAnyPosition() throws Exception {
    StreamEvaluator evaluator = factory.constructEvaluator("or(a,b,c)");

    values.clear();
    values.put("a", false);
    values.put("b", false);
    values.put("c", null);
    expectThrows(IOException.class, () -> evaluator.evaluate(new Tuple(values)));

    values.put("c", "notABoolean");
    expectThrows(IOException.class, () -> evaluator.evaluate(new Tuple(values)));

    values.put("b", 1);
    values.put("c", true);
    expectThrows(IOException.class, () -> evaluator.evaluate(new Tuple(values)));
  }

  @Test
  public void orWithSubAndsBooleans() throws Exception {
    StreamEvaluator evaluator = factory.constructEvaluator("or(a,or(b,c))");
    Object result;

    values.clear();
    values.put("a", true);
    values.put("b", true);
    values.put("c", true);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", true);
    values.put("b", true);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", true);
    values.put("b", false);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", false);
    values.put("b", true);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(true, result);

    values.clear();
    values.put("a", false);
    values.put("b", false);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Boolean);
    assertEquals(false, result);
  }
}

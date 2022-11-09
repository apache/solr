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

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.AndEvaluator;
import org.apache.solr.client.solrj.io.eval.RawValueEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

public class RawValueEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;

  public RawValueEvaluatorTest() {
    super();

    factory =
        new StreamFactory()
            .withFunctionName("val", RawValueEvaluator.class)
            .withFunctionName("add", AddEvaluator.class)
            .withFunctionName("and", AndEvaluator.class);
    values = new HashMap<>();
  }

  @Test
  public void rawTypes() throws Exception {
    Tuple tuple = new Tuple(values);

    assertEquals(10L, factory.constructEvaluator("val(10)").evaluate(tuple));
    assertEquals(-10L, factory.constructEvaluator("val(-10)").evaluate(tuple));
    assertEquals(0L, factory.constructEvaluator("val(0)").evaluate(tuple));
    assertEquals(10.5, factory.constructEvaluator("val(10.5)").evaluate(tuple));
    assertEquals(-10.5, factory.constructEvaluator("val(-10.5)").evaluate(tuple));
    assertEquals(true, factory.constructEvaluator("val(true)").evaluate(tuple));
    assertEquals(false, factory.constructEvaluator("val(false)").evaluate(tuple));
    assertNull(factory.constructEvaluator("val(null)").evaluate(tuple));
  }

  public void rawTypesAsPartOfOther() throws Exception {
    Tuple tuple = new Tuple(values);

    assertEquals(15L, factory.constructEvaluator("add(val(10),val(5))").evaluate(tuple));
    assertEquals(true, factory.constructEvaluator("and(val(true),val(true))").evaluate(tuple));
    assertEquals(false, factory.constructEvaluator("and(val(false),val(false))").evaluate(tuple));
  }
}

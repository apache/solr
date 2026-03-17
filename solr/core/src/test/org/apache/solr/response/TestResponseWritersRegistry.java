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
package org.apache.solr.response;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/**
 * This test validates the registry's behavior for built-in response writers, including
 * availability, fallback behavior, and proper format handling. Notice there is no core configured!
 */
public class TestResponseWritersRegistry extends SolrTestCaseJ4 {

  @Test
  public void testBuiltInWriterFallbackBehavior() {
    QueryResponseWriter defaultWriter = ResponseWritersRegistry.getWriter("json");

    // Test null fallback
    QueryResponseWriter nullWriter = ResponseWritersRegistry.getWriter(null);
    assertThat("null writer should be same as default", nullWriter, is(defaultWriter));

    // Test empty string fallback
    QueryResponseWriter emptyWriter = ResponseWritersRegistry.getWriter("");
    assertThat("empty writer should not be null", emptyWriter, is(not(nullValue())));
    assertThat("empty writer should be same as default", emptyWriter, is(defaultWriter));

    // Test unknown format fallback
    QueryResponseWriter unknownWriter = ResponseWritersRegistry.getWriter("nonexistent");
    assertThat("unknown writer should be null", unknownWriter, is(nullValue()));
  }
}

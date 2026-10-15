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

package org.apache.solr.handler.designer;

import java.util.List;
import java.util.Locale;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Pins the field-type inference of {@link DefaultSchemaSuggester} for exponent samples. The
 * suggester shares the parse helper with the ParseDouble update processor, so samples whose
 * exponent carries a plus sign or a lowercase marker infer {@code pdouble}, matching what the
 * processor accepts when the suggested schema is used.
 */
public class DefaultSchemaSuggesterTest extends SolrTestCase {

  @Test
  public void testGuessFieldTypeExponentFormsInferDouble() {
    DefaultSchemaSuggester suggester = new DefaultSchemaSuggester();
    // an exponent plus sign and a lowercase exponent marker both infer pdouble
    assertEquals(
        "pdouble", suggester.guessFieldType(List.of("4.5E+10", "1.0e3"), false, Locale.ROOT));
    // control: a plain uppercase exponent infers pdouble with or without the shared helper
    assertEquals("pdouble", suggester.guessFieldType(List.of("4.5E10"), false, Locale.ROOT));
  }

  @Test
  public void testGuessFieldTypeMixedSamplesFallBackToString() {
    DefaultSchemaSuggester suggester = new DefaultSchemaSuggester();
    // one non-numeric sample in the set defeats the numeric inference
    assertEquals("string", suggester.guessFieldType(List.of("4.5E+10", "abc"), false, Locale.ROOT));
  }
}

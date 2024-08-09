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
package org.apache.solr.request.macro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.common.params.CommonParams;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

public class MacroSanitizerTest {

  @Test
  public void shouldReturnSameInstanceWhenNotSanitizing() {
    // given
    Map<String, String[]> params = new HashMap<>();

    // when
    Map<String, String[]> sanitized = MacroSanitizer.sanitize(CommonParams.FQ, params);

    // then
    assertSame(params, sanitized);
  }

  @Test
  public void shouldNotSanitizeNonMacros() {
    // given
    Map<String, String[]> params = new HashMap<>();
    params.put(
        CommonParams.FQ,
        new String[] {
          "bee:up", "look:left", "{!collapse tag=collapsing field=bee sort=${collapseSort}}"
        });
    params.put("q", new String[] {"bee:honey"});

    // when
    Map<String, String[]> sanitized = MacroSanitizer.sanitize(CommonParams.FQ, params);

    // then
    assertEquals(2, sanitized.size());
    assertEquals(2, sanitized.get("fq").length);
    MatcherAssert.assertThat(sanitized.get("fq"), Matchers.arrayContaining("bee:up", "look:left"));
  }
}

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

package org.apache.solr.common.util;

import static org.apache.solr.common.util.ValidatingJsonMap.ENUM_OF;
import static org.apache.solr.common.util.ValidatingJsonMap.NOT_NULL;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;

public class TestValidatingJsonMap extends SolrTestCaseJ4 {

  public void testBasic() throws Exception {
    ValidatingJsonMap m =
        ValidatingJsonMap.wrap(
            Map.of(
                "a",
                Boolean.TRUE,
                "b",
                Boolean.FALSE,
                "i",
                10,
                "l",
                Arrays.asList("X", "Y"),
                "c",
                Map.of("d", "D")));
    assertEquals(Boolean.TRUE, m.getBool("a", Boolean.FALSE));
    assertEquals(Boolean.FALSE, m.getBool("b", Boolean.TRUE));
    assertEquals(Integer.valueOf(10), m.getInt("i", 0));

    expectThrows(RuntimeException.class, () -> m.getList("l", ENUM_OF, Set.of("X", "Z")));

    List<?> l = m.getList("l", ENUM_OF, Set.of("X", "Y", "Z"));
    assertEquals(2, l.size());
    m.getList("l", NOT_NULL);
    assertNotNull(m.getMap("c"));
  }
}

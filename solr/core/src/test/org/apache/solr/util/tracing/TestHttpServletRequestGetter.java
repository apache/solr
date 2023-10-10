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

package org.apache.solr.util.tracing;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.eclipse.jetty.server.Request;
import org.junit.Test;

public class TestHttpServletRequestGetter extends SolrTestCase {

  @Test
  public void test() {
    Map<String, Set<String>> headers =
        Map.of(
            "a", Set.of("0"),
            "b", Set.of("1"),
            "c", Set.of("2"));

    HttpServletRequest req =
        new HttpServletRequestWrapper(new Request(null, null)) {

          @Override
          public String getHeader(String name) {
            return headers.get(name).iterator().next();
          }

          @Override
          public Enumeration<String> getHeaderNames() {
            return Collections.enumeration(headers.keySet());
          }
        };

    HttpServletRequestGetter httpServletRequestGetter = new HttpServletRequestGetter();
    Iterator<String> it = httpServletRequestGetter.keys(req).iterator();
    Map<String, Set<String>> resultBack = new HashMap<>();
    while (it.hasNext()) {
      String key = it.next();
      resultBack
          .computeIfAbsent(key, k -> new HashSet<>())
          .add(httpServletRequestGetter.get(req, key));
    }
    assertEquals(headers, resultBack);
  }
}

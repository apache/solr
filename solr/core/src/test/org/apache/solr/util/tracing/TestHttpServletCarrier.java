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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class TestHttpServletCarrier extends SolrTestCaseJ4 {

  @Test
  public void test() {
    SolrTestCaseJ4.assumeWorkingMockito();
    HttpServletRequest req = mock(HttpServletRequest.class);
    Map<String, Set<String>> headers =
        Map.of(
            "a", Set.of("a", "b", "c"),
            "b", Set.of("a", "b"),
            "c", Set.of("a"));

    when(req.getHeaderNames()).thenReturn(Collections.enumeration(headers.keySet()));
    when(req.getHeaders(anyString()))
        .thenAnswer(
            (Answer<Enumeration<String>>)
                inv -> {
                  String key = inv.getArgument(0);
                  return Collections.enumeration(headers.get(key));
                });

    HttpServletCarrier servletCarrier = new HttpServletCarrier(req);
    Iterator<Map.Entry<String, String>> it = servletCarrier.iterator();
    Map<String, Set<String>> resultBack = new HashMap<>();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      resultBack.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(entry.getValue());
    }
    assertEquals(headers, resultBack);
  }
}

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

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class TestHttpServletCarrier extends SolrTestCaseJ4 {

  @Test
  public void test() {
    SolrTestCaseJ4.assumeWorkingMockito();
    HttpServletRequest req = mock(HttpServletRequest.class);
    MultiValuedMap<String, String> headers = new HashSetValuedHashMap<>();
    headers.put("a", "a");
    headers.put("a", "b");
    headers.put("a", "c");
    headers.put("b", "a");
    headers.put("b", "b");
    headers.put("c", "a");

    when(req.getHeaderNames()).thenReturn(IteratorUtils.asEnumeration(headers.keySet().iterator()));
    when(req.getHeaders(anyString()))
        .thenAnswer(
            (Answer<Enumeration<String>>)
                inv -> {
                  String key = inv.getArgument(0);
                  return IteratorUtils.asEnumeration(headers.get(key).iterator());
                });

    HttpServletCarrier servletCarrier = new HttpServletCarrier(req);
    Iterator<Map.Entry<String, String>> it = servletCarrier.iterator();
    MultiValuedMap<String, String> resultBack = new HashSetValuedHashMap<>();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      resultBack.put(entry.getKey(), entry.getValue());
    }
    assertEquals(headers, resultBack);
  }
}

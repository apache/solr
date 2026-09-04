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
package org.apache.solr.handler.admin;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/** Pure unit tests for {@link SystemInfoProvider}. */
public class SystemInfoProviderUnitTest extends SolrTestCase {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testNumericOsMetricsArePreservedAsNumbers() {
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(new ModifiableSolrParams());
    when(req.getCoreContainer()).thenReturn(null);

    Map<String, Object> info = new SystemInfoProvider(req).getSystemInfo();

    // The Admin UI calls .toFixed(2) on systemLoadAverage — must be a Number, not a String.
    Object loadAvg = info.get("systemLoadAverage");
    if (loadAvg != null) {
      assertTrue(
          "systemLoadAverage must be a Number, got " + loadAvg.getClass(),
          loadAvg instanceof Number);
    }
  }
}

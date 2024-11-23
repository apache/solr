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
package org.apache.solr.core;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHttpSolrClientProvider extends SolrTestCase {

  SolrMetricsContext parentSolrMetricCtx;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    assumeWorkingMockito();
    parentSolrMetricCtx = Mockito.mock(SolrMetricsContext.class);
  }

  @Test
  public void test_when_updateShardHandler_cfg_is_null() {
    try (var httpSolrClientProvider = new HttpSolrClientProvider(null, parentSolrMetricCtx); ) {
      assertEquals(
          httpSolrClientProvider.getSolrClient().getIdleTimeout(),
          HttpClientUtil.DEFAULT_SO_TIMEOUT);
    }
  }

  @Test
  public void test_when_updateShardHandler_cfg_is_not_null() {
    var idleTimeout = 10000;
    assertNotEquals(idleTimeout, UpdateShardHandlerConfig.DEFAULT.getDistributedSocketTimeout());
    UpdateShardHandlerConfig cfg = new UpdateShardHandlerConfig(-1, -1, idleTimeout, -1, null, -1);
    try (var httpSolrClientProvider = new HttpSolrClientProvider(cfg, parentSolrMetricCtx); ) {
      assertEquals(httpSolrClientProvider.getSolrClient().getIdleTimeout(), idleTimeout);
    }
  }

  @Test
  public void test_closing_solr_metric_context() {
    SolrMetricsContext childSolrMetricContext = Mockito.mock(SolrMetricsContext.class);
    Mockito.when(parentSolrMetricCtx.getChildContext(any(HttpSolrClientProvider.class)))
        .thenReturn(childSolrMetricContext);
    try (var httpSolrClientProvider = new HttpSolrClientProvider(null, parentSolrMetricCtx)) {
      assertNotNull(httpSolrClientProvider.getSolrClient());
    } finally {
      verify(childSolrMetricContext, times(1)).unregister();
    }
  }
}

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
package org.apache.solr.security;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongHistogram;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;

public final class MockSolrMetricsContextFactory {

  public static SolrMetricsContext create() {
    SolrMetricsContext mockParentContext = mock(SolrMetricsContext.class);
    SolrMetricsContext mockChildContext = mock(SolrMetricsContext.class);

    when(mockParentContext.getChildContext(any())).thenReturn(mockChildContext);

    LongCounter mockOtelLongCounter = mock(LongCounter.class);
    when(mockChildContext.longCounter(anyString(), any())).thenReturn(mockOtelLongCounter);

    LongHistogram mockLongHistogram = mock(LongHistogram.class);
    when(mockChildContext.longHistogram(anyString(), anyString(), any(OtelUnit.class)))
        .thenReturn(mockLongHistogram);

    when(mockChildContext.observableLongGauge(anyString(), anyString(), any())).thenReturn(null);
    when(mockChildContext.observableLongCounter(anyString(), anyString(), any())).thenReturn(null);

    LongGauge mockLongGauge = mock(LongGauge.class);
    when(mockChildContext.longGauge(anyString(), anyString())).thenReturn(mockLongGauge);

    return mockParentContext;
  }
}

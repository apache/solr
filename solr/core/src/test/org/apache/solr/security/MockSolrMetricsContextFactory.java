package org.apache.solr.security;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import io.opentelemetry.api.metrics.LongCounter;
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

    Timer mockTimer = mock(Timer.class);
    Timer.Context mockTimerContext = mock(Timer.Context.class);
    when(mockTimer.time()).thenReturn(mockTimerContext);
    when(mockChildContext.timer(anyString(), anyString(), anyString(), anyString())).thenReturn(mockTimer);

    Counter mockCounter = mock(Counter.class);
    when(mockChildContext.counter(anyString(), anyString(), anyString(), anyString())).thenReturn(mockCounter);

    LongHistogram mockLongHistogram = mock(LongHistogram.class);
    when(mockChildContext.longHistogram(anyString(), anyString(), any(OtelUnit.class))).thenReturn(mockLongHistogram);

    when(mockChildContext.observableLongGauge(anyString(), anyString(), any())).thenReturn(null);
    when(mockChildContext.observableLongCounter(anyString(), anyString(), any())).thenReturn(null);

    return mockParentContext;
  }
}

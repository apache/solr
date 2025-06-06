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
package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import org.apache.solr.metrics.otel.OtelDoubleMetric;

public class BoundDoubleUpDownCounter implements OtelDoubleMetric {

  private final DoubleUpDownCounter upDownCounter;
  private final Attributes attributes;

  public BoundDoubleUpDownCounter(DoubleUpDownCounter upDownCounter, Attributes attributes) {
    this.upDownCounter = upDownCounter;
    this.attributes = attributes;
  }

  public void inc() {
    record(1.0);
  }

  public void dec() {
    record(-1.0);
  }

  @Override
  public void record(Double value) {
    upDownCounter.add(value, attributes);
  }
}

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
import io.opentelemetry.api.metrics.LongCounter;

/**
 * An AttributedLongCounter that writes to both core and node registries with corresponding
 * attributes.
 */
public class DualRegistryAttributedLongCounter extends AttributedLongCounter {

  private final AttributedLongCounter nodeCounter;

  public DualRegistryAttributedLongCounter(
      LongCounter coreCounter,
      Attributes coreAttributes,
      LongCounter nodeCounter,
      Attributes nodeAttributes) {
    super(coreCounter, coreAttributes);
    this.nodeCounter = new AttributedLongCounter(nodeCounter, nodeAttributes);
  }

  @Override
  public void inc() {
    super.inc();
    nodeCounter.inc();
  }

  @Override
  public void add(Long value) {
    super.add(value);
    nodeCounter.add(value);
  }
}

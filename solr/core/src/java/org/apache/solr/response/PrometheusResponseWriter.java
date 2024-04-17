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
package org.apache.solr.response;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

@SuppressWarnings(value = "unchecked")
public class PrometheusResponseWriter extends RawResponseWriter {
  @Override
  public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse response)
      throws IOException {

    NamedList<Object> prometheusRegistries =
        (NamedList<Object>) response.getValues().get("metrics");
    Map<String, Object> registryMap = prometheusRegistries.asShallowMap();
    PrometheusTextFormatWriter prometheusTextFormatWriter = new PrometheusTextFormatWriter(false);
    registryMap.forEach(
        (name, registry) -> {
          try {
            PrometheusRegistry prometheusRegistry = (PrometheusRegistry) registry;
            prometheusTextFormatWriter.write(out, prometheusRegistry.scrape());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}

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

package org.apache.solr.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

public final class SolrJMetricTestUtils {

  public static double getPrometheusMetricValue(SolrClient solrClient, String metricName)
      throws SolrServerException, IOException {
    var req =
        new GenericSolrRequest(
            SolrRequest.METHOD.GET,
            "/admin/metrics",
            SolrRequest.SolrRequestType.ADMIN,
            SolrParams.of("wt", "prometheus"));
    req.setResponseParser(new InputStreamResponseParser("prometheus"));

    NamedList<Object> resp = solrClient.request(req);
    try (InputStream in = (InputStream) resp.get("stream")) {
      String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return output
          .lines()
          .filter(l -> l.startsWith(metricName))
          .mapToDouble(s -> Double.parseDouble(s.substring(s.lastIndexOf(" "))))
          .sum();
    }
  }

  public static Double getNumCoreRequests(
      String baseUrl, String collectionName, String category, String handler)
      throws SolrServerException, IOException {

    try (var client = new HttpJettySolrClient.Builder(baseUrl).build()) {
      var req =
          new GenericSolrRequest(
              SolrRequest.METHOD.GET,
              "/admin/metrics",
              SolrRequest.SolrRequestType.ADMIN,
              SolrParams.of("wt", "prometheus"));
      req.setResponseParser(new InputStreamResponseParser("prometheus"));

      NamedList<Object> resp = client.request(req);
      try (InputStream in = (InputStream) resp.get("stream")) {
        String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        String metricName;

        metricName = "solr_core_requests_total";

        return output
            .lines()
            .filter(
                l ->
                    l.contains(metricName)
                        && l.contains("category=\"" + category + "\"")
                        && l.contains("collection=\"" + collectionName + "\"")
                        && l.contains("handler=\"" + handler + "\""))
            .mapToDouble(s -> Double.parseDouble(s.substring(s.lastIndexOf(" "))))
            .sum();
      }
    }
  }

  public static Double getNumNodeRequestErrors(String baseUrl, String category, String handler)
      throws SolrServerException, IOException {

    try (var client = new HttpJettySolrClient.Builder(baseUrl).build()) {
      var req =
          new GenericSolrRequest(
              SolrRequest.METHOD.GET,
              "/admin/metrics",
              SolrRequest.SolrRequestType.ADMIN,
              SolrParams.of("wt", "prometheus"));
      req.setResponseParser(new InputStreamResponseParser("prometheus"));

      NamedList<Object> resp = client.request(req);
      try (InputStream in = (InputStream) resp.get("stream")) {
        String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        String metricName;
        metricName = "solr_node_requests_errors_total";
        // Sum both client and server errors
        return output
            .lines()
            .filter(
                l ->
                    l.contains(metricName)
                        && l.contains("category=\"" + category + "\"")
                        && l.contains("handler=\"" + handler + "\""))
            .mapToDouble(s -> Double.parseDouble(s.substring(s.lastIndexOf(" "))))
            .sum();
      }
    }
  }
}

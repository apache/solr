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

package org.apache.solr.prometheus.exporter;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.zookeeper.client.ConnectStringParser;

public class SolrClientFactory {

  private final PrometheusExporterSettings settings;
  private final SolrScrapeConfiguration configuration;

  public SolrClientFactory(
      PrometheusExporterSettings settings, SolrScrapeConfiguration configuration) {
    this.settings = settings;
    this.configuration = configuration;
  }

  private static Http2SolrClient.Builder newHttp2SolrClientBuilder(
      String solrHost, PrometheusExporterSettings settings, SolrScrapeConfiguration configuration) {
    var builder =
        new Http2SolrClient.Builder(solrHost)
            .withIdleTimeout(settings.getHttpReadTimeout(), TimeUnit.MILLISECONDS)
            .withConnectionTimeout(settings.getHttpConnectionTimeout(), TimeUnit.MILLISECONDS)
            .withResponseParser(new NoOpResponseParser("json"));
    if (configuration.getBasicAuthUser() != null) {
      builder.withBasicAuthCredentials(
          configuration.getBasicAuthUser(), configuration.getBasicAuthPwd());
    }
    return builder;
  }

  public Http2SolrClient createStandaloneSolrClient(String solrHost) {
    return newHttp2SolrClientBuilder(solrHost, settings, configuration).build();
  }

  public CloudSolrClient createCloudSolrClient(String zookeeperConnectionString) {
    ConnectStringParser parser = new ConnectStringParser(zookeeperConnectionString);

    List<String> zkHosts =
        parser.getServerAddresses().stream()
            .map(address -> address.getHostString() + ":" + address.getPort())
            .collect(Collectors.toList());
    CloudSolrClient client =
        new CloudHttp2SolrClient.Builder(zkHosts, Optional.ofNullable(parser.getChrootPath()))
            .withInternalClientBuilder(newHttp2SolrClientBuilder(null, settings, configuration))
            .withResponseParser(new NoOpResponseParser("json"))
            .build();

    client.connect();

    return client;
  }
}

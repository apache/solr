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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.client.solrj.request.MetricsRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

/** SOLR-18400: /admin/metrics must not 500 when metrics collection is disabled. */
public class TestPrometheusResponseWriterMetricsDisabled extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    // metrics collection is disabled by default in MiniSolrCloudCluster
    configureCluster(1).configure();
  }

  @Test
  public void testMetricsDisabledReturnsComment() throws Exception {
    var req = new MetricsRequest(SolrParams.of("wt", "prometheus"));

    NamedList<Object> resp = cluster.getSolrClient().request(req);
    assertEquals(200, resp.get("responseStatus"));
    try (InputStream in = (InputStream) resp.get("stream")) {
      String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("# metrics collection is disabled\n", output);
    }
  }
}

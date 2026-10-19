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

import org.apache.solr.client.solrj.request.MetricsRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Both the v1 {@code /admin/metrics} and the v2 {@code /api/metrics} endpoint answer HTTP 510 when
 * metrics collection is switched off in solr.xml.
 */
public class MetricsDisabledTest extends SolrCloudTestCase {

  private static final String METRICS_V2_PATH = "/metrics";

  @BeforeClass
  public static void setupCluster() throws Exception {
    // MiniSolrCloudCluster's default solr.xml has <metrics enabled="false">
    configureCluster(1).configure();
  }

  @Test
  public void testV1MetricsDisabled() throws Exception {
    assertMetricsDisabled(CommonParams.METRICS_PATH);
  }

  @Test
  public void testV2MetricsDisabled() throws Exception {
    assertMetricsDisabled(METRICS_V2_PATH);
  }

  private static void assertMetricsDisabled(String path) throws Exception {
    var req = new MetricsRequest(path, SolrParams.of(CommonParams.WT, "prometheus"));

    NamedList<Object> resp = cluster.getSolrClient().request(req);
    String body = InputStreamResponseParser.consumeResponseToString(resp);

    assertEquals(
        "Expected HTTP 510 from " + path,
        SolrException.ErrorCode.INVALID_STATE.code,
        (int) (Integer) resp.get(InputStreamResponseParser.HTTP_STATUS_KEY));
    assertTrue(body, body.contains("Metrics collection is disabled"));
  }
}

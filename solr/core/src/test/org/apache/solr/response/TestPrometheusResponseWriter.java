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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests the {@link PrometheusResponseWriter} behavior */
public class TestPrometheusResponseWriter extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema.xml");
    h.getCoreContainer().waitForLoadingCoresToFinish(30000);
  }

  @Test
  public void testPrometheusOutput() throws Exception {
    SolrQueryRequest req = req("dummy");
    SolrQueryResponse rsp = new SolrQueryResponse();
    PrometheusResponseWriter w = new PrometheusResponseWriter();
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());
    SolrQueryResponse resp = new SolrQueryResponse();

    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "prometheus"),
        resp);
    NamedList<?> values = resp.getValues();
    rsp.add("metrics", values.get("metrics"));
    w.write(byteOut, req, rsp);
    String actualOutput = byteOut.toString(StandardCharsets.UTF_8).replaceAll("(?<=}).*", "");
    String expectedOutput =
        Files.readString(
            new File(TEST_PATH().toString(), "solr-prometheus-output.txt").toPath(),
            StandardCharsets.UTF_8);
    assertEquals(expectedOutput, actualOutput);
    handler.close();
  }
}

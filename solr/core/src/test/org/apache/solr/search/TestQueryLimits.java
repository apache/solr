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
package org.apache.solr.search;

import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.XMLWriter;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestQueryLimits extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "test";

  private static Path createConfigSet(
      boolean withExpensiveComponent, NamedList<Object> defaults, NamedList<Object> invariants)
      throws Exception {
    Path configSet = createTempDir();
    copyMinConf(configSet.toFile());
    // insert an expensive search component
    Path solrConfig = configSet.resolve("conf/solrconfig.xml");
    String xmlExpensiveSection =
        withExpensiveComponent
            ? "\n    <arr name=\"first-components\">\n"
                + "      <str>expensiveSearchComponent</str>\n"
                + "    </arr>\n"
            : "";
    String configStr = Files.readString(solrConfig);
    if (defaults != null && defaults.size() > 0) {
      StringWriter sw = new StringWriter();
      XMLWriter writer =
          new XMLWriter(
              sw,
              new SolrQueryRequestBase(null, new ModifiableSolrParams()) {},
              new SolrQueryResponse());
      writer.writeNamedList("defaults", defaults);
      writer.close();
      String xmlDefaultsSection = sw.toString();
      // there's already a defaults section in our copy - replace it
      configStr =
          configStr.replaceAll(
              "(?s)SearchHandler\">\\s+<lst name=\"defaults.*?</lst>",
              "SearchHandler\">" + xmlDefaultsSection);
    }
    String xmlInvariantsSection;
    if (invariants != null && invariants.size() > 0) {
      StringWriter sw = new StringWriter();
      XMLWriter writer =
          new XMLWriter(
              sw,
              new SolrQueryRequestBase(null, new ModifiableSolrParams()) {},
              new SolrQueryResponse());
      writer.writeNamedList("invariants", invariants);
      writer.close();
      xmlInvariantsSection = sw.toString();
    } else {
      xmlInvariantsSection = "";
    }
    Files.writeString(
        solrConfig,
        configStr
            .replace(
                "<requestHandler",
                "<searchComponent name=\"expensiveSearchComponent\"\n"
                    + "                   class=\"org.apache.solr.search.ExpensiveSearchComponent\"/>\n"
                    + "\n"
                    + "  <requestHandler")
            .replace(
                "class=\"solr.SearchHandler\">",
                "class=\"solr.SearchHandler\">" + xmlExpensiveSection + xmlInvariantsSection));
    return configSet.resolve("conf");
  }

  @After
  public void cleanup() throws Exception {
    shutdownCluster();
  }

  private void setupCluster(
      boolean withExpensive, NamedList<Object> defaults, NamedList<Object> invariants)
      throws Exception {
    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");
    Path configset = createConfigSet(withExpensive, defaults, invariants);
    configureCluster(1).addConfig("conf", configset).configure();
    SolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 2);
    create.process(solrClient);
    CloudUtil.waitForState(
        cluster.getOpenOverseer().getSolrCloudManager(), "active", COLLECTION, clusterShape(3, 6));
    for (int j = 0; j < 100; j++) {
      solrClient.add(COLLECTION, sdoc("id", "id-" + j, "val_i", j % 5));
    }
    solrClient.commit(COLLECTION);
  }

  @Test
  public void testDefaults() throws Exception {
    NamedList<Object> defaults = new NamedList<>();
    defaults.add("timeAllowed", "100");
    setupCluster(true, defaults, null);
    SolrClient solrClient = cluster.getSolrClient();

    // no delay, should return full results
    QueryResponse rsp = solrClient.query(COLLECTION, params("q", "id:*", "sort", "id asc"));
    assertEquals(rsp.getHeader().get("status"), 0);
    assertNull("should not have partial results", rsp.getHeader().get("partialResults"));

    // delay exceeds defaults, should return partial results
    rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                "500",
                ExpensiveSearchComponent.STAGES_PARAM,
                "process"));
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));

    // delay exceeds defaults but override provided, should return full results
    rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                "500",
                ExpensiveSearchComponent.STAGES_PARAM,
                "process",
                CommonParams.TIME_ALLOWED,
                "1000"));
    assertNull("should not have partial results", rsp.getHeader().get("partialResults"));
  }

  @Test
  public void testInvariants() throws Exception {
    NamedList<Object> invariants = new NamedList<>();
    invariants.add("timeAllowed", "100");
    setupCluster(true, null, invariants);
    SolrClient solrClient = cluster.getSolrClient();

    // no delay, should return full results
    QueryResponse rsp = solrClient.query(COLLECTION, params("q", "id:*", "sort", "id asc"));
    assertEquals(rsp.getHeader().get("status"), 0);
    assertNull("should not have partial results", rsp.getHeader().get("partialResults"));

    // delay exceeds invariants, should return partial results
    rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                "500",
                ExpensiveSearchComponent.STAGES_PARAM,
                "process"));
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));

    // delay exceeds invariants, override provided, but it should be ignored and still return partial results
    rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                "500",
                ExpensiveSearchComponent.STAGES_PARAM,
                "process",
                CommonParams.TIME_ALLOWED,
                "1000"));
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));
  }
}

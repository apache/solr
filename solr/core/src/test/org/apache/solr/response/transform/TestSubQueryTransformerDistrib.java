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
package org.apache.solr.response.transform;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.junit.BeforeClass;
import org.junit.Test;

@org.apache.solr.SolrTestCaseJ4.SuppressSSL()
public class TestSubQueryTransformerDistrib extends SolrCloudTestCase {

  private static final String support = "These guys help customers";
  private static final String engineering = "These guys develop stuff";
  static final String people = "people";
  static final String depts = "departments";
  private static boolean differentUniqueId;
  private static final String USER = "solr";
  private static final String PASS = "SolrRocksAgain";

  @BeforeClass
  public static void setupCluster() throws Exception {

    differentUniqueId = random().nextBoolean();

    final Path configDir = TEST_COLL1_CONF();

    String configName = "solrCloudCollectionConfig";
    int nodeCount = 5;

    final String SECURITY_JSON =
        Utils.toJSONString(
            Map.of(
                "authorization",
                Map.of(
                    "class",
                    RuleBasedAuthorizationPlugin.class.getName(),
                    "user-role",
                    singletonMap(USER, "admin"),
                    "permissions",
                    singletonList(Map.of("name", "all", "role", "admin"))),
                "authentication",
                Map.of(
                    "class",
                    BasicAuthPlugin.class.getName(),
                    "blockUnknown",
                    true,
                    "credentials",
                    singletonMap(USER, getSaltedHashedValue(PASS)),
                    "forwardCredentials", // forward basic auth credentials during internode
                    // requests instead of relying on PKI authentication; the test should pass
                    // regardless of the setting here, but "true" is the more interesting case that
                    // demonstrates that basic auth credentials are being properly handled in the
                    // context of a distributed subquery
                    true)));

    configureCluster(nodeCount)
        .addConfig(configName, configDir)
        .withSecurityJson(SECURITY_JSON)
        .configure();

    int shards = 2;
    int replicas = 2;
    withBasicAuth(
            CollectionAdminRequest.createCollection(people, configName, shards, replicas)
                .withProperty("config", "solrconfig-doctransformers.xml")
                .withProperty("schema", "schema-docValuesJoin.xml"))
        .process(cluster.getSolrClient());

    withBasicAuth(
            CollectionAdminRequest.createCollection(depts, configName, shards, replicas)
                .withProperty("config", "solrconfig-doctransformers.xml")
                .withProperty(
                    "schema",
                    differentUniqueId
                        ? "schema-minimal-with-another-uniqkey.xml"
                        : "schema-docValuesJoin.xml"))
        .process(cluster.getSolrClient());

    CloudSolrClient client = cluster.getSolrClient();

    ZkStateReader zkStateReader = ZkStateReader.from(client);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(people, zkStateReader, true, true, 30);

    AbstractDistribZkTestBase.waitForRecoveriesToFinish(depts, zkStateReader, false, true, 30);
  }

  @SuppressWarnings("serial")
  @Test
  public void test() throws Exception {
    int peopleMultiplier = atLeast(1);
    int deptMultiplier = atLeast(1);

    createIndex(people, peopleMultiplier, depts, deptMultiplier);

    Random random1 = random();

    final ModifiableSolrParams params =
        params(
            new String[] {
              "q",
              "name_s:dave",
              "indent",
              "true",
              "fl",
              "*,depts:[subquery " + ((random1.nextBoolean() ? "" : "separator=,")) + "]",
              "rows",
              "" + peopleMultiplier,
              "depts.q",
              "{!terms f=dept_id_s v=$row.dept_ss_dv "
                  + ((random1.nextBoolean() ? "" : "separator=,"))
                  + "}",
              "depts.fl",
              "text_t" + (differentUniqueId ? ",id:notid" : ""),
              "depts.sort",
              "dept_id_i desc",
              "depts.indent",
              "true",
              "depts.collection",
              "departments",
              differentUniqueId ? "depts.distrib.singlePass" : "notnecessary",
              "true",
              "depts.rows",
              "" + (deptMultiplier * 2),
              "depts.logParamsList",
              "q,fl,rows,row.dept_ss_dv",
              random().nextBoolean() ? "depts.wt" : "whatever",
              anyWt(),
              random().nextBoolean() ? "wt" : "whatever",
              anyWt()
            });

    final SolrDocumentList hits;
    {
      final QueryRequest qr = withBasicAuth(new QueryRequest(params));
      final QueryResponse rsp = new QueryResponse();
      rsp.setResponse(cluster.getSolrClient().request(qr, people + "," + depts));
      hits = rsp.getResults();

      assertEquals(peopleMultiplier, hits.getNumFound());

      int engineerCount = 0;
      int supportCount = 0;

      for (int res : new int[] {0, (peopleMultiplier - 1) / 2, peopleMultiplier - 1}) {
        SolrDocument doc = hits.get(res);
        assertEquals("dave", doc.getFieldValue("name_s_dv"));
        SolrDocumentList relDepts = (SolrDocumentList) doc.getFieldValue("depts");
        assertEquals("dave works in both depts " + rsp, deptMultiplier * 2, relDepts.getNumFound());
        for (int deptN = 0; deptN < relDepts.getNumFound(); deptN++) {
          SolrDocument deptDoc = relDepts.get(deptN);
          String actual = (String) deptDoc.get("text_t");
          assertTrue(
              deptDoc + "should be either " + engineering + " or " + support,
              (engineering.equals(actual) && ++engineerCount > 0)
                  || (support.equals(actual) && ++supportCount > 0));
        }
      }
      assertEquals(hits.toString(), engineerCount, supportCount);
    }

    params.set("wt", "json");
    final URL node =
        URI.create(
                cluster.getRandomJetty(random()).getBaseUrl().toString()
                    + "/"
                    + people
                    + "/select"
                    + params.toQueryString())
            .toURL();

    final URLConnection urlConnectionWithoutAuth = node.openConnection();
    assertThrows(Exception.class, () -> urlConnectionWithoutAuth.getInputStream());
    final URLConnection urlConnection = node.openConnection();
    String basicAuth =
        "Basic "
            + new String(
                Base64.getEncoder().encode((USER + ":" + PASS).getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);
    urlConnection.setRequestProperty("Authorization", basicAuth);

    try (final InputStream jsonResponse = urlConnection.getInputStream()) {
      final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
      jsonResponse.transferTo(outBuffer);

      final Object expected =
          ((SolrDocumentList) hits.get(0).getFieldValue("depts")).get(0).get("text_t");
      final String err =
          JSONTestUtil.match(
              "/response/docs/[0]/depts/docs/[0]/text_t",
              outBuffer.toString(StandardCharsets.UTF_8),
              "\"" + expected + "\"");
      assertNull(err, err);
    }
  }

  private String anyWt() {
    String[] wts = new String[] {"javabin", "xml", "json"};
    return wts[random().nextInt(wts.length)];
  }

  private void createIndex(String people, int peopleMultiplier, String depts, int deptMultiplier)
      throws SolrServerException, IOException {

    int id = 0;
    List<String> peopleDocs = new ArrayList<>();
    for (int p = 0; p < peopleMultiplier; p++) {

      peopleDocs.add(
          add(
              doc(
                  "id",
                  "" + id++,
                  "name_s",
                  "john",
                  "title_s",
                  "Director",
                  "dept_ss_dv",
                  "Engineering",
                  "dept_i",
                  "0",
                  "dept_is",
                  "0")));
      peopleDocs.add(
          add(
              doc(
                  "id",
                  "" + id++,
                  "name_s",
                  "mark",
                  "title_s",
                  "VP",
                  "dept_ss_dv",
                  "Marketing",
                  "dept_i",
                  "1",
                  "dept_is",
                  "1")));
      peopleDocs.add(
          add(
              doc(
                  "id",
                  "" + id++,
                  "name_s",
                  "nancy",
                  "title_s",
                  "MTS",
                  "dept_ss_dv",
                  "Sales",
                  "dept_i",
                  "2",
                  "dept_is",
                  "2")));
      peopleDocs.add(
          add(
              doc(
                  "id",
                  "" + id++,
                  "name_s",
                  "dave",
                  "title_s",
                  "MTS",
                  "dept_ss_dv",
                  "Support",
                  "dept_ss_dv",
                  "Engineering",
                  "dept_i",
                  "3",
                  "dept_is",
                  "3",
                  "dept_is",
                  "0")));
      peopleDocs.add(
          add(
              doc(
                  "id",
                  "" + id++,
                  "name_s",
                  "tina",
                  "title_s",
                  "VP",
                  "dept_ss_dv",
                  "Engineering",
                  "dept_i",
                  "0",
                  "dept_is",
                  "0")));
    }

    addDocs(people, peopleDocs);

    List<String> deptsDocs = new ArrayList<>();
    String deptIdField = differentUniqueId ? "notid" : "id";
    for (int d = 0; d < deptMultiplier; d++) {
      deptsDocs.add(
          add(
              doc(
                  deptIdField,
                  "" + id++,
                  "dept_id_s",
                  "Engineering",
                  "text_t",
                  engineering,
                  "salary_i_dv",
                  "1000",
                  "dept_id_i",
                  "0")));
      deptsDocs.add(
          add(
              doc(
                  deptIdField,
                  "" + id++,
                  "dept_id_s",
                  "Marketing",
                  "text_t",
                  "These guys make you look good",
                  "salary_i_dv",
                  "1500",
                  "dept_id_i",
                  "1")));
      deptsDocs.add(
          add(
              doc(
                  deptIdField,
                  "" + id++,
                  "dept_id_s",
                  "Sales",
                  "text_t",
                  "These guys sell stuff",
                  "salary_i_dv",
                  "1600",
                  "dept_id_i",
                  "2")));
      deptsDocs.add(
          add(
              doc(
                  deptIdField,
                  "" + id++,
                  "dept_id_s",
                  "Support",
                  "text_t",
                  support,
                  "salary_i_dv",
                  "800",
                  "dept_id_i",
                  "3")));
    }
    addDocs(depts, deptsDocs);
  }

  private void addDocs(String collection, List<String> docs)
      throws SolrServerException, IOException {
    StringBuilder upd = new StringBuilder("<update>");

    upd.append("<delete><query>*:*</query></delete>");

    for (Iterator<String> iterator = docs.iterator(); iterator.hasNext(); ) {
      String add = iterator.next();
      upd.append(add);
      if (rarely()) {
        upd.append(commit("softCommit", "true"));
      }
      if (rarely() || !iterator.hasNext()) {
        if (!iterator.hasNext()) {
          upd.append(commit("softCommit", "false"));
        }
        upd.append("</update>");

        ContentStreamUpdateRequest req = withBasicAuth(new ContentStreamUpdateRequest("/update"));
        req.addContentStream(new ContentStreamBase.StringStream(upd.toString(), "text/xml"));
        cluster.getSolrClient().request(req, collection);
        upd.setLength("<update>".length());
      }
    }
  }

  private static <T extends SolrRequest<? extends SolrResponse>> T withBasicAuth(T req) {
    req.setBasicAuthCredentials(USER, PASS);
    return req;
  }
}

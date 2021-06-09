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

package org.apache.solr.handler.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

public class SQLWithAuthzEnabledTest extends SolrCloudTestCase {

  private static final String ADMIN_USER = "solr";
  private static final String ADMIN_PASS = "Admin!!";
  private static final String SQL_USER = "sql";
  private static final String SQL_PASS = "SolrRocks!!";
  private static final String collectionName = "testSQLWithAuthz";

  @BeforeClass
  public static void setupClusterWithSecurityEnabled() throws Exception {
    final String SECURITY_JSON = Utils.toJSONString
        (Map.of("authorization",
            Map.of("class", RuleBasedAuthorizationPlugin.class.getName(),
                "user-role", Map.of(SQL_USER, "sql", ADMIN_USER, "admin"),
                "permissions", Arrays.asList(
                    Map.of("name", "sql", "role", "sql", "path", "/sql", "collection", "*"),
                    Map.of("name", "export", "role", "sql", "path", "/export", "collection", "*"),
                    Map.of("name", "all", "role", "admin"))
            ),
            "authentication",
            Map.of("class", BasicAuthPlugin.class.getName(),
                "blockUnknown", true,
                "credentials", Map.of(SQL_USER, getSaltedHashedValue(SQL_PASS), ADMIN_USER, getSaltedHashedValue(ADMIN_PASS)))));

    configureCluster(2)
        .addConfig("conf", configset("sql"))
        .withSecurityJson(SECURITY_JSON)
        .configure();
  }

  private <T extends SolrRequest<? extends SolrResponse>> T doAsAdmin(T req) {
    req.setBasicAuthCredentials(ADMIN_USER, ADMIN_PASS);
    return req;
  }

  @Test
  public void testSqlAuthz() throws Exception {
    doAsAdmin(CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2, 0, 0)).process(cluster.getSolrClient());
    waitForState("Expected collection to be created with 1 shards and 2 replicas", collectionName, clusterShape(1, 2));

    int maxDocs = 10;
    CloudSolrClient solrClient = cluster.getSolrClient();
    UpdateRequest updateRequest = doAsAdmin(new UpdateRequest());
    for (int i = 0; i < maxDocs; i++) {
      updateRequest.add(new SolrInputDocument("id", String.valueOf(i)));
    }
    updateRequest.commit(solrClient, collectionName);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/sql");
    params.set("stmt", "select id from " + collectionName);
    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + collectionName;
    SolrStream solrStream = new SolrStream(baseUrl, params);
    solrStream.setCredentials(SQL_USER, SQL_PASS);
    List<Tuple> tuples = getTuples(solrStream);
    assertEquals(maxDocs, tuples.size());
  }

  List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new LinkedList<>();
    try (tupleStream) {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
      }
    }
    return tuples;
  }
}

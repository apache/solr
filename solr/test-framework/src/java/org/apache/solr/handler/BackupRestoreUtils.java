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

package org.apache.solr.handler;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.http.client.utils.URIBuilder;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupRestoreUtils extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static int indexDocs(SolrClient leaderClient, String collectionName, long docsSeed)
      throws IOException, SolrServerException {
    leaderClient.deleteByQuery(collectionName, "*:*");

    Random random =
        new Random(
            docsSeed); // use a constant seed for the whole test run so that we can easily re-index.
    int nDocs = TestUtil.nextInt(random, 1, 100);
    log.info("Indexing {} test docs", nDocs);

    List<SolrInputDocument> docs = new ArrayList<>(nDocs);
    for (int i = 0; i < nDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", "name = " + i);
      docs.add(doc);
    }
    leaderClient.add(collectionName, docs);
    leaderClient.commit(collectionName);
    verifyDocs(nDocs, leaderClient, collectionName);
    return nDocs;
  }

  public static void verifyDocs(int nDocs, SolrClient leaderClient, String collectionName)
      throws SolrServerException, IOException {
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "*:*");
    QueryResponse response = leaderClient.query(collectionName, queryParams);

    assertEquals(0, response.getStatus());
    assertEquals(nDocs, response.getResults().getNumFound());
  }

  public static void runCoreAdminCommand(
      String baseUrl, String coreName, String action, Map<String, String> params)
      throws IOException, URISyntaxException {
    final URI uri = new URI(baseUrl);
    final var oldPath = uri.getPath() != null ? uri.getPath().substring(1) : "";
    final var newPath = "admin/cores";
    final var finalPath = oldPath.isEmpty() ? newPath : oldPath + "/" + newPath;

    final URIBuilder builder =
        new URIBuilder(uri)
            .setPath(finalPath)
            .addParameter("action", action)
            .addParameter("core", coreName);

    // Add additional parameters using loop
    for (Map.Entry<String, String> entry : params.entrySet()) {
      builder.addParameter(entry.getKey(), entry.getValue());
    }

    executeHttpRequest(builder.build());
  }

  public static void runReplicationHandlerCommand(
      String baseUrl, String coreName, String action, String repoName, String backupName)
      throws IOException, URISyntaxException {
    final URI uri = new URI(baseUrl);
    final var oldPath = uri.getPath() != null ? uri.getPath().substring(1) : "";
    final var newPath = coreName + ReplicationHandler.PATH;
    final var finalPath = oldPath.isEmpty() ? newPath : oldPath + "/" + newPath;

    final URI finalURI =
        new URIBuilder(uri)
            .setPath(finalPath)
            .addParameter("command", action)
            .addParameter("repository", repoName)
            .addParameter("name", backupName)
            .build();
    executeHttpRequest(finalURI);
  }

  private static void executeHttpRequest(URI uri) throws IOException {
    URL url = uri.toURL();
    try (InputStream stream = url.openStream()) {
      assert stream != null;
    }
  }
}

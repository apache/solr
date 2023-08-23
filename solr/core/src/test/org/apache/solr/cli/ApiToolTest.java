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
package org.apache.solr.cli;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Locale;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class ApiToolTest extends SolrCloudTestCase {
  static String COLLECTION_NAME = "globalLoaderColl";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void testParsingGetUrl() throws URISyntaxException {

    String urlWithPercent20SpaceDelimiter =
        "http://localhost:8983/solr/COLL_NAME/sql?stmt=select%20id%20from%20COLL_NAME%20limit%2010";
    String urlWithPlusSpaceDelimiter =
        "http://localhost:8983/solr/COLL_NAME/sql?stmt=select+id+from+COLL_NAME+limit+10";

    for (String url : Arrays.asList(urlWithPercent20SpaceDelimiter, urlWithPlusSpaceDelimiter)) {
      url = url.replace("+", "%20");

      URI uri = new URI(url);
      ModifiableSolrParams params = ApiTool.getSolrParamsFromUri(uri);
      assertEquals(1, params.size());
      assertEquals("select id from COLL_NAME limit 10", params.get("stmt"));
    }
  }

  @Test
  public void testQueryResponse() throws Exception {
    int docCount = 1000;
    CollectionAdminRequest.createCollection(COLLECTION_NAME, "config", 2, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);

    String tmpFileLoc =
        new File(cluster.getBaseDir().toFile().getAbsolutePath() + File.separator).getPath();

    UpdateRequest ur = new UpdateRequest();
    ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);

    for (int i = 0; i < docCount; i++) {
      ur.add(
          "id",
          String.valueOf(i),
          "desc_s",
          TestUtil.randomSimpleString(random(), 10, 50),
          "a_dt",
          "2019-09-30T05:58:03Z");
    }
    cluster.getSolrClient().request(ur, COLLECTION_NAME);

    ApiTool tool = new ApiTool();

    String response =
        tool.callGet(
            cluster.getJettySolrRunner(0).getBaseUrl()
                + "/"
                + COLLECTION_NAME
                + "/select?q=*:*&rows=1&fl=id&sort=id+asc");
    // Fields that could be missed because of serialization
    assertFindInJson(response, "\"numFound\":1000,");
    // Correct formatting
    assertFindInJson(response, "\"docs\":[{");
  }

  private void assertFindInJson(String json, String find) {
    assertTrue(
        String.format(Locale.ROOT, "Could not find string %s in response: \n%s", find, json),
        json.contains(find));
  }
}

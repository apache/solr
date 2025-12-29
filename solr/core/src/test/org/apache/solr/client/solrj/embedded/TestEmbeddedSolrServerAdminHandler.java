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
package org.apache.solr.client.solrj.embedded;

import java.io.IOException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SystemInfoRequest;
import org.apache.solr.client.solrj.response.SystemInfoResponse;
import org.apache.solr.core.NodeConfig;
import org.junit.Test;

public class TestEmbeddedSolrServerAdminHandler extends SolrTestCaseJ4 {

  @Test
  public void testPathIsAddedToContext() throws IOException, SolrServerException {

    final NodeConfig config =
        new NodeConfig.NodeConfigBuilder("testnode", TEST_PATH())
            .setConfigSetBaseDirectory(TEST_PATH().resolve("configsets").toString())
            .build();

    try (final EmbeddedSolrServer server = new EmbeddedSolrServer(config, "collection1")) {
      final SystemInfoRequest info = new SystemInfoRequest();
      final SystemInfoResponse response = info.process(server);
      assertTrue(response.getResponse().size() > 0);
    }
  }
}

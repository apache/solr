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

import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.junit.BeforeClass;

/**
 * A subclass of SolrExampleTests that explicitly uses the HTTP2 client and the xml codec for
 * communication.
 */
public class SolrExampleXMLHttp2Test extends SolrExampleTests {
  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Override
  public SolrClient createNewSolrClient() {

    Http2SolrClient client =
        new Http2SolrClient.Builder(getCoreUrl())
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withRequestWriter(new RequestWriter())
            .withResponseParser(new XMLResponseParser())
            .build();
    return client;
  }
}

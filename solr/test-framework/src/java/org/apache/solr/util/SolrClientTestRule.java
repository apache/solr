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
package org.apache.solr.util;

import java.io.IOException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.junit.rules.ExternalResource;

/**
 * Provides access to a {@link SolrClient} instance and a running Solr in tests. Implementations
 * could run Solr in different ways (e.g. strictly embedded, adding HTTP/Jetty, adding SolrCloud, or
 * an external process). It's a JUnit {@link ExternalResource} (a {@code TestRule}), and thus closes
 * the client and Solr itself when the test completes. This test utility is encouraged to be used by
 * external projects that wish to test communicating with Solr, especially for plugin providers.
 */
public abstract class SolrClientTestRule extends ExternalResource {

  public abstract SolrClient getSolrClient();

  public abstract SolrClient getSolrClient(String name);

  public void clearIndex() throws SolrServerException, IOException {
    new UpdateRequest().deleteByQuery("*:*").commit(getSolrClient(), null);
  }
}

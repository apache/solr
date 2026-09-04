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

import static org.junit.Assert.assertNull;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.BaseTestHarness;

final class LukeTestUtil {

  private LukeTestUtil() {}

  static void assertLukeXPath(
      SolrClient client, String collection, SolrParams extra, String... xpaths) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("shards.info", "true");
    params.add(extra);
    LukeRequest req = new LukeRequest(params);
    req.setNumTerms(0);
    req.setResponseParser(new InputStreamResponseParser("xml"));
    NamedList<Object> raw =
        collection != null ? client.request(req, collection) : client.request(req);
    String xml = InputStreamResponseParser.consumeResponseToString(raw);
    String failedXpath = BaseTestHarness.validateXPath(xml, xpaths);
    assertNull("XPath validation failed: " + failedXpath + "\nResponse:\n" + xml, failedXpath);
  }
}

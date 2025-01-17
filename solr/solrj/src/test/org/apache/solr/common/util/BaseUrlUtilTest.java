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
package org.apache.solr.common.util;

import static org.apache.solr.common.util.Utils.getBaseUrlForNodeName;
import static org.apache.solr.common.util.Utils.getNodeNameFromSolrUrl;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class BaseUrlUtilTest extends SolrTestCase {

  @Test
  public void testGetNodeNameFromSolrUrl() throws MalformedURLException, URISyntaxException {
    assertEquals("node-1-url:8983_solr", getNodeNameFromSolrUrl("https://node-1-url:8983/solr"));
    assertEquals("node-1-url:8983_solr", getNodeNameFromSolrUrl("http://node-1-url:8983/solr"));
    assertEquals("node-1-url:8983_api", getNodeNameFromSolrUrl("http://node-1-url:8983/api"));
    assertThrows(MalformedURLException.class, () -> getNodeNameFromSolrUrl("node-1-url:8983/solr"));
    assertThrows(
        URISyntaxException.class, () -> getNodeNameFromSolrUrl("http://node-1-url:8983/solr^"));
  }

  @Test
  public void testGetBaseUrlForNodeName() {
    assertEquals(
        "http://app-node-1:8983/solr",
        getBaseUrlForNodeName("app-node-1:8983_solr", "http", false));
    assertEquals(
        "https://app-node-1:8983/solr",
        getBaseUrlForNodeName("app-node-1:8983_solr", "https", false));
    assertEquals(
        "http://app-node-1:8983/api", getBaseUrlForNodeName("app-node-1:8983_solr", "http", true));
    assertEquals(
        "https://app-node-1:8983/api",
        getBaseUrlForNodeName("app-node-1:8983_solr", "https", true));
  }
}

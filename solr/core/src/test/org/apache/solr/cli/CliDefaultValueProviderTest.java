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

import org.apache.solr.SolrTestCase;
import org.junit.Test;
import picocli.CommandLine;

public class CliDefaultValueProviderTest extends SolrTestCase {

  private static String defaultFor(String optionName, String paramLabel) throws Exception {
    var spec = CommandLine.Model.OptionSpec.builder(optionName).paramLabel(paramLabel).build();
    return new CliDefaultValueProvider().defaultValue(spec);
  }

  @Test
  public void testConnectionDefaultsFromSystemProperties() throws Exception {
    System.setProperty("solr.connection", "zk1:2181/solr");
    System.setProperty("zkHost", "zk2:2181/solr");
    System.setProperty("solr.url", "http://solr.local:8983");
    assertEquals("zk1:2181/solr", defaultFor("--solr-connection", "<solrConnection>"));
    assertEquals("zk2:2181/solr", defaultFor("--zk-host", "<zkHost>"));
    assertEquals("http://solr.local:8983", defaultFor("--solr-url", "<solrUrl>"));
  }

  @Test
  public void testNoDefaultWhenPropertiesUnset() throws Exception {
    System.clearProperty("solr.connection");
    System.clearProperty("zkHost");
    System.clearProperty("solr.url");
    assertNull(defaultFor("--solr-connection", "<solrConnection>"));
    assertNull(defaultFor("--zk-host", "<zkHost>"));
    assertNull(defaultFor("--solr-url", "<solrUrl>"));
    assertNull(defaultFor("--other", "<other>"));
  }
}

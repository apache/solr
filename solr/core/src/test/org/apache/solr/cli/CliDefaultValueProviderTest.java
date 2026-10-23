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

  @CommandLine.Command(name = "shared")
  static class SharedProbe {
    @CommandLine.Mixin ConnectionOptions connection;
  }

  /**
   * Like {@code ApiTool}: its own required {@code --solr-url} is a full endpoint, not a base URL.
   */
  @CommandLine.Command(name = "local")
  static class LocalUrlProbe {
    @CommandLine.Option(names = "--solr-url", required = true)
    String solrUrl;

    @CommandLine.Option(names = "--zk-host")
    String zkHost;
  }

  private static <T> T parse(T command, String... args) {
    new CommandLine(command).setDefaultValueProvider(new CliDefaultValueProvider()).parseArgs(args);
    return command;
  }

  private static String defaultFor(String optionName) throws Exception {
    return defaultFor(optionName, "<value>");
  }

  private static String defaultFor(String optionName, String paramLabel) throws Exception {
    var spec = CommandLine.Model.OptionSpec.builder(optionName).paramLabel(paramLabel).build();
    return new CliDefaultValueProvider().defaultValue(spec);
  }

  @Test
  public void testConnectionDefaultsFromSystemProperties() {
    System.setProperty("solr.connection", "zk1:2181/solr");
    System.setProperty("zkHost", "zk2:2181/solr");
    System.setProperty("solr.url", "http://solr.local:8983");
    SharedProbe probe = parse(new SharedProbe());
    assertEquals("zk1:2181/solr", probe.connection.solrConnection);
    assertEquals("zk2:2181/solr", probe.connection.zkHost);
    assertEquals("http://solr.local:8983", probe.connection.solrUrl);
  }

  @Test
  public void testKeyedByOptionNameNotParamLabel() throws Exception {
    System.setProperty("zkHost", "zk2:2181/solr");
    assertEquals("zk2:2181/solr", defaultFor("--zk-host", "ZK"));
    assertNull(defaultFor("--other", "<zkHost>"));
  }

  @Test
  public void testShortNameAlone() throws Exception {
    System.setProperty("zkHost", "zk2:2181/solr");
    var spec = CommandLine.Model.OptionSpec.builder("-z", "--zk-host").build();
    assertEquals("zk2:2181/solr", new CliDefaultValueProvider().defaultValue(spec));
  }

  @Test
  public void testNoDefaultWhenPropertiesUnset() {
    System.clearProperty("solr.connection");
    System.clearProperty("zkHost");
    System.clearProperty("solr.url");
    SharedProbe probe = parse(new SharedProbe());
    assertNull(probe.connection.solrConnection);
    assertNull(probe.connection.zkHost);
    assertNull(probe.connection.solrUrl);
  }

  @Test
  public void testLocalSolrUrlOptionGetsNoDefault() {
    System.setProperty("solr.url", "http://solr.local:8983");
    System.setProperty("zkHost", "zk2:2181/solr");
    var commandLine =
        new CommandLine(new LocalUrlProbe()).setDefaultValueProvider(new CliDefaultValueProvider());
    var e =
        expectThrows(CommandLine.MissingParameterException.class, () -> commandLine.parseArgs());
    assertTrue(e.getMessage(), e.getMessage().contains("--solr-url"));

    LocalUrlProbe probe = parse(new LocalUrlProbe(), "--solr-url", "http://given:8983/api/x");
    assertEquals("http://given:8983/api/x", probe.solrUrl);
    assertEquals("zk2:2181/solr", probe.zkHost);
  }

  @Test
  public void testPortAndMaxWaitDefaults() throws Exception {
    System.clearProperty("solr.port.listen");
    System.clearProperty("solr.max.wait.seconds");
    assertEquals("8983", defaultFor("--port"));
    assertEquals("0", defaultFor("--max-wait-secs"));
    System.setProperty("solr.port.listen", "7574");
    assertEquals("7574", defaultFor("--port"));
  }

  @Test
  public void testPositionalGetsNoDefault() throws Exception {
    System.setProperty("solr.url", "http://solr.local:8983");
    var spec = CommandLine.Model.PositionalParamSpec.builder().paramLabel("<solrUrl>").build();
    assertNull(new CliDefaultValueProvider().defaultValue(spec));
  }
}

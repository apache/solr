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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.solr.SolrTestCase;
import org.junit.Test;
import picocli.CommandLine;

/** Parsing-level tests of {@link ConnectionOptions}; nothing here contacts Solr or ZooKeeper. */
public class ConnectionOptionsTest extends SolrTestCase {

  @CommandLine.Command(name = "probe")
  static class Probe implements Callable<Integer> {
    @CommandLine.Mixin ConnectionOptions connection;

    @Override
    public Integer call() throws Exception {
      connection.resolveSolrUrl(null, false);
      return 0;
    }
  }

  /** Mirrors the tools that have no meaningful default target, e.g. {@code export}. */
  @CommandLine.Command(name = "requiring-probe")
  static class RequiringProbe implements Callable<Integer> {
    @CommandLine.Mixin ConnectionOptions connection;

    @Override
    public Integer call() {
      connection.requireExplicitConnection();
      return 0;
    }
  }

  private static Probe parse(String... args) {
    Probe probe = new Probe();
    new CommandLine(probe).setDefaultValueProvider(new CliDefaultValueProvider()).parseArgs(args);
    return probe;
  }

  @Test
  public void testExplicitUrlBeatsEnvironmentZkHost() throws Exception {
    System.setProperty("zkHost", "zk1:2181/solr");
    Probe probe = parse("--solr-url", "http://host:8983/solr/");
    assertTrue(probe.connection.hasExplicitConnection());
    assertNull(probe.connection.namedConnection());
    assertEquals("http://host:8983", probe.connection.resolveSolrUrl(null));
  }

  @Test
  public void testEnvironmentValueIsNotExplicit() throws Exception {
    System.setProperty("solr.url", "http://env:8983");
    Probe probe = parse();
    assertFalse(probe.connection.hasExplicitConnection());
    assertEquals("http://env:8983", probe.connection.resolveSolrUrl(null));
  }

  @Test
  public void testEnvironmentPrecedence() throws Exception {
    System.setProperty("solr.connection", "http://conn:8983");
    System.setProperty("zkHost", "zk1:2181/solr");
    System.setProperty("solr.url", "http://url:8983");
    Probe probe = parse();
    assertEquals("http://conn:8983", probe.connection.resolveSolrUrl(null));
    assertFalse(probe.connection.namedConnection().isZookeeper());

    System.clearProperty("solr.connection");
    probe = parse();
    var named = probe.connection.namedConnection();
    assertTrue(named.isZookeeper());
    assertEquals(List.of("zk1:2181"), named.quorumItems());
    assertEquals("zk1:2181/solr", probe.connection.resolveZkHost(null));

    System.clearProperty("zkHost");
    probe = parse();
    assertNull(probe.connection.namedConnection());
    assertEquals("http://url:8983", probe.connection.resolveSolrUrl(null));
  }

  @Test
  public void testTwoExplicitOptionsAreRejected() {
    Probe probe = parse("--zk-host", "zk1:2181", "--solr-url", "http://host:8983");
    var e =
        expectThrows(
            CommandLine.ParameterException.class, () -> probe.connection.resolveSolrUrl(null));
    assertTrue(e.getMessage(), e.getMessage().contains("mutually exclusive"));

    StringWriter err = new StringWriter();
    int exitCode =
        new CommandLine(new Probe())
            .setErr(new PrintWriter(err))
            .execute("--zk-host", "zk1:2181", "--solr-url", "http://host:8983");
    assertEquals(CommandLine.ExitCode.USAGE, exitCode);
    assertTrue(err.toString(), err.toString().contains("mutually exclusive"));
  }

  @Test
  public void testExclusivityIsAUsageErrorInRealTools() {
    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    StringWriter err = new StringWriter();
    int exitCode =
        new CommandLine(new VersionTool(runtime))
            .setErr(new PrintWriter(err))
            .execute("--zk-host", "zk1:2181", "--solr-url", "http://host:8983");
    assertEquals(CommandLine.ExitCode.USAGE, exitCode);
    assertTrue(err.toString(), err.toString().contains("mutually exclusive"));
  }

  @Test
  public void testMissingConnectionIsAUsageError() {
    System.clearProperty("solr.connection");
    System.clearProperty("zkHost");
    System.clearProperty("solr.url");
    StringWriter err = new StringWriter();
    int exitCode =
        new CommandLine(new RequiringProbe())
            .setErr(new PrintWriter(err))
            .execute("--solr-url", "http://host:8983");
    assertEquals(CommandLine.ExitCode.OK, exitCode);

    err = new StringWriter();
    exitCode = new CommandLine(new RequiringProbe()).setErr(new PrintWriter(err)).execute();
    assertEquals(CommandLine.ExitCode.USAGE, exitCode);
    assertTrue(err.toString(), err.toString().contains("Missing required connection target"));
    assertTrue(err.toString(), err.toString().contains("Usage:"));
  }

  @Test
  public void testNonZkZkHostIsRejected() {
    Probe probe = parse("--zk-host", "http://host:8983");
    expectThrows(IOException.class, () -> probe.connection.namedConnection());
  }

  @Test
  public void testDefaultWhenNothingGiven() throws Exception {
    System.clearProperty("solr.connection");
    System.clearProperty("zkHost");
    System.clearProperty("solr.url");
    Probe probe = parse();
    assertFalse(probe.connection.hasExplicitConnection());
    assertNull(probe.connection.namedConnection());
    assertEquals(CLIUtils.getDefaultSolrUrl(), probe.connection.resolveSolrUrl(null, false));
  }

  @Test
  public void testWorksWithoutParseResult() throws Exception {
    ConnectionOptions options = new ConnectionOptions();
    options.solrUrl = "http://direct:8983";
    assertFalse(options.hasExplicitConnection());
    assertEquals("http://direct:8983", options.resolveSolrUrl(null));
  }
}

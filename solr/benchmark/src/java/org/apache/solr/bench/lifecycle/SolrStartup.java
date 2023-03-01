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

package org.apache.solr.bench.lifecycle;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/**
 * A simple JMH benchmark that attempts to measure approximate Solr startup behavior by measuring
 * {@link JettySolrRunner#start()}
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(time = 60, iterations = 10)
@Threads(3)
@Fork(value = 1)
public class SolrStartup {

  @Benchmark
  public void startSolr(PerThreadState threadState) throws Exception {
    // 'false' tells Jetty to not go out of its way to reuse any previous ports
    threadState.solrRunner.start(false);
  }

  @State(Scope.Thread)
  public static class PerThreadState {

    private static final int NUM_CORES = 10;

    public Path tmpSolrHome;
    public JettySolrRunner solrRunner;

    @Setup(Level.Trial)
    public void bootstrapJettyServer() throws Exception {
      tmpSolrHome = Files.createTempDirectory("solrstartup-perthreadstate-jsr").toAbsolutePath();

      final Path configsetsDir = tmpSolrHome.resolve("configsets");
      final Path defaultConfigsetDir = configsetsDir.resolve("defaultConfigSet");
      Files.createDirectories(defaultConfigsetDir);
      PathUtils.copyDirectory(
          Path.of("src/resources/configs/minimal/conf"), defaultConfigsetDir.resolve("conf"));
      PathUtils.copyFileToDirectory(Path.of("src/resources/solr.xml"), tmpSolrHome);

      solrRunner = new JettySolrRunner(tmpSolrHome.toString(), buildJettyConfig("/solr"));
      solrRunner.start(false);
      try (SolrClient client = solrRunner.newClient()) {
        for (int i = 0; i < NUM_CORES; i++) {
          createCore(client, "core-prefix-" + i);
        }
      }
      solrRunner.stop();
    }

    private void createCore(SolrClient client, String coreName) throws Exception {
      CoreAdminRequest.Create create = new CoreAdminRequest.Create();
      create.setCoreName(coreName);
      create.setConfigSet("defaultConfigSet");

      final CoreAdminResponse response = create.process(client);
      if (response.getStatus() != 0) {
        throw new RuntimeException("Some error creating core: " + response.jsonStr());
      }
    }

    @TearDown(Level.Invocation)
    public void stopJettyServerIfNecessary() throws Exception {
      if (solrRunner.isRunning()) {
        solrRunner.stop();
      }
    }

    @TearDown(Level.Trial)
    public void destroyJettyServer() throws Exception {
      if (solrRunner.isRunning()) {
        solrRunner.stop();
      }

      IOUtils.rm(tmpSolrHome);
    }

    private static JettyConfig buildJettyConfig(String context) {
      return JettyConfig.builder().setContext(context).stopAtShutdown(true).build();
    }
  }
}

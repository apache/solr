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
package org.apache.solr.bench;

import static org.apache.commons.io.file.PathUtils.deleteDirectory;

import com.codahale.metrics.Meter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Control;

/** The base class for Solr JMH benchmarks that operate against a {@link MiniSolrCloudCluster}. */
public class MiniClusterState {

  public static final boolean DEBUG_OUTPUT = false;

  private static final long RANDOM_SEED = 6624420638116043983L;

  public static final int PROC_COUNT =
      ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

  private static boolean quietLog = Boolean.getBoolean("quietLog");

  @SuppressForbidden(reason = "JMH uses std out for user output")
  public static void log(String value) {
    if (!quietLog) {
      System.out.println((value.equals("") ? "" : "--> ") + value);
    }
  }

  @State(Scope.Benchmark)
  public static class MiniClusterBenchState {

    boolean metricsEnabled = true;

    public List<String> nodes;
    MiniSolrCloudCluster cluster;
    public SolrClient client;

    int runCnt = 0;

    boolean createCollectionAndIndex = true;

    boolean deleteMiniCluster = true;

    Path baseDir;
    boolean allowClusterReuse = false;

    boolean isWarmup;

    private SplittableRandom random;

    @TearDown(Level.Iteration)
    public void tearDown(BenchmarkParams benchmarkParams) throws Exception {

      // dump Solr metrics
      Path metricsResults =
          Paths.get(
              "work/metrics-results",
              benchmarkParams.id(),
              String.valueOf(runCnt++),
              benchmarkParams.getBenchmark() + ".txt");
      if (!Files.exists(metricsResults.getParent())) {
        Files.createDirectories(metricsResults.getParent());
      }

      cluster.outputMetrics(
          metricsResults.getParent().toFile(), metricsResults.getFileName().toString());
    }

    @Setup(Level.Iteration)
    public void checkWarmUp(Control control) throws Exception {
      isWarmup = control.stopMeasurement;
    }

    @TearDown(Level.Trial)
    public void shutdownMiniCluster() throws Exception {
      if (DEBUG_OUTPUT) log("closing client and shutting down minicluster");
      IOUtils.closeQuietly(client);
      cluster.shutdown();
    }

    @Setup(Level.Trial)
    public void doSetup(BenchmarkParams benchmarkParams) throws Exception {

      MiniClusterState.log("");
      Path currentRelativePath = Paths.get("");
      String s = currentRelativePath.toAbsolutePath().toString();
      log("current relative path is: " + s);

      Long seed = Long.getLong("solr.bench.seed");

      if (seed == null) {
        seed = RANDOM_SEED;
      }

      log("benchmark random seed: " + seed);

      // set the seed used by ThreadLocalRandom
      System.setProperty("randomSeed", Long.toString(new Random(seed).nextLong()));

      this.random = new SplittableRandom(seed);

      System.setProperty("pkiHandlerPrivateKeyPath", "");
      System.setProperty("pkiHandlerPublicKeyPath", "");

      System.setProperty("solr.log.name", benchmarkParams.id());

      System.setProperty("solr.default.confdir", "../server/solr/configsets/_default");

      // not currently usable, but would enable JettySolrRunner's ill-conceived jetty.testMode and
      // allow using SSL

      // System.getProperty("jetty.testMode", "true");
      // SolrCloudTestCase.sslConfig = SolrTestCaseJ4.buildSSLConfig();

      String baseDirSysProp = System.getProperty("miniClusterBaseDir");
      if (baseDirSysProp != null) {
        deleteMiniCluster = false;
        baseDir = Paths.get(baseDirSysProp);
        if (Files.exists(baseDir)) {
          createCollectionAndIndex = false;
          allowClusterReuse = true;
        }
      } else {
        baseDir = Paths.get("work/mini-cluster");
      }

      System.setProperty("metricsEnabled", String.valueOf(metricsEnabled));
    }

    public void metricsEnabled(boolean metricsEnabled) {
      this.metricsEnabled = metricsEnabled;
    }

    public void startMiniCluster(int nodeCount) {
      log("starting mini cluster at base directory: " + baseDir.toAbsolutePath());

      if (!allowClusterReuse && Files.exists(baseDir)) {
        log("mini cluster base directory exists, removing ...");
        try {
          deleteDirectory(baseDir);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        createCollectionAndIndex = true;
      } else if (Files.exists(baseDir)) {
        createCollectionAndIndex = false;
        deleteMiniCluster = false;
      }

      try {
        cluster =
            new MiniSolrCloudCluster.Builder(nodeCount, baseDir)
                .formatZkServer(false)
                .addConfig("conf", Paths.get("src/resources/configs/cloud-minimal/conf"))
                .configure();
      } catch (Exception e) {
        if (Files.exists(baseDir)) {
          try {
            deleteDirectory(baseDir);
          } catch (IOException ex) {
            e.addSuppressed(ex);
          }
        }
        throw new RuntimeException(e);
      }

      nodes = new ArrayList<>(nodeCount);
      List<JettySolrRunner> jetties = cluster.getJettySolrRunners();
      for (JettySolrRunner runner : jetties) {
        nodes.add(runner.getBaseUrl().toString());
      }

      client = new Http2SolrClient.Builder().build();

      log("done starting mini cluster");
      log("");
    }

    public SplittableRandom getRandom() {
      return random;
    }

    public void createCollection(String collection, int numShards, int numReplicas)
        throws Exception {
      if (createCollectionAndIndex) {
        try {

          CollectionAdminRequest.Create request =
              CollectionAdminRequest.createCollection(collection, "conf", numShards, numReplicas);
          request.setBasePath(nodes.get(random.nextInt(cluster.getJettySolrRunners().size())));

          client.request(request);

          cluster.waitForActiveCollection(
              collection, 15, TimeUnit.SECONDS, numShards, numShards * numReplicas);
        } catch (Exception e) {
          if (Files.exists(baseDir)) {
            deleteDirectory(baseDir);
          }
          throw e;
        }
      }
    }

    @SuppressForbidden(reason = "This module does not need to deal with logging context")
    public void index(String collection, DocMaker docMaker, int docCount) throws Exception {
      if (createCollectionAndIndex) {

        log("indexing data for benchmark...");
        Meter meter = new Meter();
        ExecutorService executorService =
            Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new SolrNamedThreadFactory("SolrJMH Indexer Progress"));
        ScheduledExecutorService scheduledExecutor =
            Executors.newSingleThreadScheduledExecutor(
                new SolrNamedThreadFactory("SolrJMH Indexer"));
        scheduledExecutor.scheduleAtFixedRate(
            () -> {
              if (meter.getCount() == docCount) {
                scheduledExecutor.shutdown();
              } else {
                log(meter.getCount() + " docs at " + meter.getMeanRate() + " doc/s");
              }
            },
            10,
            10,
            TimeUnit.SECONDS);
        for (int i = 0; i < docCount; i++) {
          executorService.submit(
              new Runnable() {
                SplittableRandom threadRandom = random.split();

                @Override
                public void run() {
                  UpdateRequest updateRequest = new UpdateRequest();
                  updateRequest.setBasePath(
                      nodes.get(threadRandom.nextInt(cluster.getJettySolrRunners().size())));
                  SolrInputDocument doc = docMaker.getDocument(threadRandom);
                  // log("add doc " + doc);
                  updateRequest.add(doc);
                  meter.mark();

                  try {
                    client.request(updateRequest, collection);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }
              });
        }

        log("done adding docs, waiting for executor to terminate...");

        executorService.shutdown();
        boolean result = executorService.awaitTermination(600, TimeUnit.MINUTES);

        scheduledExecutor.shutdown();

        if (!result) {
          throw new RuntimeException("Timeout waiting for doc adds to finish");
        }
        log("done indexing data for benchmark");

        log("committing data ...");
        UpdateRequest commitRequest = new UpdateRequest();
        commitRequest.setBasePath(nodes.get(random.nextInt(cluster.getJettySolrRunners().size())));
        commitRequest.setAction(UpdateRequest.ACTION.COMMIT, false, true);
        commitRequest.process(client, collection);
        log("done committing data");
      } else {
        cluster.waitForActiveCollection(collection, 15, TimeUnit.SECONDS);
      }

      QueryRequest queryRequest = new QueryRequest(new SolrQuery("q", "*:*", "rows", "1"));
      queryRequest.setBasePath(nodes.get(random.nextInt(cluster.getJettySolrRunners().size())));

      NamedList<Object> result = client.request(queryRequest, collection);

      if (DEBUG_OUTPUT) MiniClusterState.log("result: " + result);

      MiniClusterState.log("");

      MiniClusterState.log("Dump Core Info");
      dumpCoreInfo();
    }

    public void waitForMerges(String collection) throws Exception {
      forceMerge(collection, Integer.MAX_VALUE);
    }

    public void forceMerge(String collection, int maxMergeSegments) throws Exception {
      if (createCollectionAndIndex) {
        // we control segment count for a more informative benchmark *and* because background
        // merging would continue after
        // indexing and overlap with the benchmark
        if (maxMergeSegments == Integer.MAX_VALUE) {
          log("waiting for merges to finish...\n");
        } else {
          log("merging segments to " + maxMergeSegments + " segments ...\n");
        }

        UpdateRequest optimizeRequest = new UpdateRequest();
        optimizeRequest.setBasePath(
            nodes.get(random.nextInt(cluster.getJettySolrRunners().size())));
        optimizeRequest.setAction(UpdateRequest.ACTION.OPTIMIZE, false, true, maxMergeSegments);
        optimizeRequest.process(client, collection);
      }
    }

    @SuppressForbidden(reason = "JMH uses std out for user output")
    public void dumpCoreInfo() throws IOException {
      cluster.dumpCoreInfo(!quietLog ? System.out : new NullPrintStream());
    }
  }

  public static ModifiableSolrParams params(ModifiableSolrParams params, String... moreParams) {
    for (int i = 0; i < moreParams.length; i += 2) {
      params.add(moreParams[i], moreParams[i + 1]);
    }
    return params;
  }
}

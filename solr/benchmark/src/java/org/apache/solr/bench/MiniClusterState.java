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
import static org.apache.solr.bench.BaseBenchState.log;

import com.codahale.metrics.Meter;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.solr.util.SolrTestNonSecureRandomProvider;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Control;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The base class for Solr JMH benchmarks that operate against a {@code MiniSolrCloudCluster}. */
public class MiniClusterState {

  /** The constant DEBUG_OUTPUT. */
  public static final boolean DEBUG_OUTPUT = false;

  /** The constant PROC_COUNT. */
  public static final int PROC_COUNT =
      ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** The type Mini cluster bench state. */
  @State(Scope.Benchmark)
  public static class MiniClusterBenchState {

    /** The Metrics enabled. */
    boolean metricsEnabled = true;

    /** The Nodes. */
    public List<String> nodes;

    /** The Cluster. */
    MiniSolrCloudCluster cluster;

    /** The Client. */
    public SolrClient client;

    /** The Run cnt. */
    int runCnt = 0;

    /** The Create collection and index. */
    boolean createCollectionAndIndex = true;

    /** The Delete mini cluster. */
    boolean deleteMiniCluster = true;

    /** Unless overridden we ensure SecureRandoms do not block. */
    boolean doNotWeakenSecureRandom = Boolean.getBoolean("doNotWeakenSecureRandom");

    /** The Mini cluster base dir. */
    Path miniClusterBaseDir;

    /** To Allow cluster reuse. */
    boolean allowClusterReuse = false;

    /** The Is warmup. */
    boolean isWarmup;

    private SplittableRandom random;
    private String workDir;

    /**
     * Tear down.
     *
     * @param benchmarkParams the benchmark params
     * @throws Exception the exception
     */
    @TearDown(Level.Iteration)
    public void tearDown(BenchmarkParams benchmarkParams) throws Exception {

      // dump Solr metrics
      Path metricsResults =
          Paths.get(
              workDir,
              "metrics-results",
              benchmarkParams.id(),
              String.valueOf(runCnt++),
              benchmarkParams.getBenchmark() + ".txt");
      Files.createDirectories(metricsResults.getParent());

      cluster.dumpMetrics(
          metricsResults.getParent().toFile(), metricsResults.getFileName().toString());
    }

    /**
     * Check warm up.
     *
     * @param control the control
     * @throws Exception the exception
     */
    @Setup(Level.Iteration)
    public void checkWarmUp(Control control) throws Exception {
      isWarmup = control.stopMeasurement;
    }

    /**
     * Shutdown mini cluster.
     *
     * @param benchmarkParams the benchmark params
     * @throws Exception the exception
     */
    @TearDown(Level.Trial)
    public void shutdownMiniCluster(BenchmarkParams benchmarkParams, BaseBenchState baseBenchState)
        throws Exception {

      log.info("MiniClusterState tear down - baseBenchState is {}", baseBenchState);

      BaseBenchState.dumpHeap(benchmarkParams);

      if (DEBUG_OUTPUT) log("closing client and shutting down minicluster");
      IOUtils.closeQuietly(client);
      cluster.shutdown();
    }

    /**
     * Do setup.
     *
     * @param benchmarkParams the benchmark params
     * @param baseBenchState the base bench state
     * @throws Exception the exception
     */
    @Setup(Level.Trial)
    public void doSetup(BenchmarkParams benchmarkParams, BaseBenchState baseBenchState)
        throws Exception {

      if (!doNotWeakenSecureRandom) {
        // remove all blocking from all secure randoms
        SolrTestNonSecureRandomProvider.injectProvider();
      }

      workDir = System.getProperty("workBaseDir", "build/work");

      log("");
      Path currentRelativePath = Paths.get("");
      String s = currentRelativePath.toAbsolutePath().toString();
      log("current relative path is: " + s);
      log("work path is: " + workDir);

      System.setProperty("doNotWaitForMergesOnIWClose", "true");

      System.setProperty("pkiHandlerPrivateKeyPath", "");
      System.setProperty("pkiHandlerPublicKeyPath", "");

      System.setProperty("solr.default.confdir", "../server/solr/configsets/_default");

      this.random = new SplittableRandom(BaseBenchState.getRandomSeed());

      // not currently usable, but would enable JettySolrRunner's ill-conceived jetty.testMode and
      // allow using SSL

      // System.getProperty("jetty.testMode", "true");
      // SolrCloudTestCase.sslConfig = SolrTestCaseJ4.buildSSLConfig();

      String baseDirSysProp = System.getProperty("miniClusterBaseDir");
      if (baseDirSysProp != null) {
        deleteMiniCluster = false;
        miniClusterBaseDir = Paths.get(baseDirSysProp);
        if (Files.exists(miniClusterBaseDir)) {
          createCollectionAndIndex = false;
          allowClusterReuse = true;
        }
      } else {
        miniClusterBaseDir = Paths.get(workDir, "mini-cluster");
      }

      System.setProperty("metricsEnabled", String.valueOf(metricsEnabled));
    }

    /**
     * Metrics enabled.
     *
     * @param metricsEnabled the metrics enabled
     */
    public void metricsEnabled(boolean metricsEnabled) {
      this.metricsEnabled = metricsEnabled;
    }

    /**
     * Start mini cluster.
     *
     * @param nodeCount the node count
     */
    public void startMiniCluster(int nodeCount) {
      log("starting mini cluster at base directory: " + miniClusterBaseDir.toAbsolutePath());

      if (!allowClusterReuse && Files.exists(miniClusterBaseDir)) {
        log("mini cluster base directory exists, removing ...");
        try {
          deleteDirectory(miniClusterBaseDir);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        createCollectionAndIndex = true;
      } else if (Files.exists(miniClusterBaseDir)) {
        createCollectionAndIndex = false;
        deleteMiniCluster = false;
      }

      try {
        cluster =
            new MiniSolrCloudCluster.Builder(nodeCount, miniClusterBaseDir)
                .formatZkServer(false)
                .addConfig("conf", getFile("src/resources/configs/cloud-minimal/conf").toPath())
                .configure();
      } catch (Exception e) {
        if (Files.exists(miniClusterBaseDir)) {
          try {
            deleteDirectory(miniClusterBaseDir);
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

    /**
     * Gets random.
     *
     * @return the random
     */
    public SplittableRandom getRandom() {
      return random;
    }

    /**
     * Create collection.
     *
     * @param collection the collection
     * @param numShards the num shards
     * @param numReplicas the num replicas
     * @throws Exception the exception
     */
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
          if (Files.exists(miniClusterBaseDir)) {
            deleteDirectory(miniClusterBaseDir);
          }
          throw e;
        }
      }
    }

    /**
     * Index.
     *
     * @param collection the collection
     * @param docs the docs
     * @param docCount the doc count
     * @throws Exception the exception
     */
    @SuppressForbidden(reason = "This module does not need to deal with logging context")
    public void index(String collection, Docs docs, int docCount) throws Exception {
      if (createCollectionAndIndex) {

        log("indexing data for benchmark...");
        Meter meter = new Meter();
        ExecutorService executorService =
            Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new SolrNamedThreadFactory("SolrJMH Indexer"));
        ScheduledExecutorService scheduledExecutor =
            Executors.newSingleThreadScheduledExecutor(
                new SolrNamedThreadFactory("SolrJMH Indexer Progress"));
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
                final SplittableRandom threadRandom = random.split();

                @Override
                public void run() {
                  UpdateRequest updateRequest = new UpdateRequest();
                  updateRequest.setBasePath(
                      nodes.get(threadRandom.nextInt(cluster.getJettySolrRunners().size())));
                  SolrInputDocument doc = docs.inputDocument();
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
        boolean result = false;
        while (!result) {
          result = executorService.awaitTermination(600, TimeUnit.MINUTES);
        }

        scheduledExecutor.shutdown();

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

      if (DEBUG_OUTPUT) log("result: " + result);

      log("");

      log("Dump Core Info");
      dumpCoreInfo();
    }

    /**
     * Wait for merges.
     *
     * @param collection the collection
     * @throws Exception the exception
     */
    public void waitForMerges(String collection) throws Exception {
      forceMerge(collection, Integer.MAX_VALUE);
    }

    /**
     * Force merge.
     *
     * @param collection the collection
     * @param maxMergeSegments the max merge segments
     * @throws Exception the exception
     */
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

    /**
     * Dump core info.
     *
     * @throws IOException the io exception
     */
    @SuppressForbidden(reason = "JMH uses std out for user output")
    public void dumpCoreInfo() throws IOException {
      cluster.dumpCoreInfo(!BaseBenchState.QUIET_LOG ? System.out : new NullPrintStream());
    }
  }

  /**
   * Params modifiable solr params.
   *
   * @param moreParams the more params
   * @return the modifiable solr params
   */
  public static ModifiableSolrParams params(String... moreParams) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (int i = 0; i < moreParams.length; i += 2) {
      params.add(moreParams[i], moreParams[i + 1]);
    }
    return params;
  }

  /**
   * Params modifiable solr params.
   *
   * @param params the params
   * @param moreParams the more params
   * @return the modifiable solr params
   */
  public static ModifiableSolrParams params(ModifiableSolrParams params, String... moreParams) {
    for (int i = 0; i < moreParams.length; i += 2) {
      params.add(moreParams[i], moreParams[i + 1]);
    }
    return params;
  }

  /**
   * Gets file.
   *
   * @param name the name
   * @return the file
   */
  public static File getFile(String name) {
    final URL url =
        MiniClusterState.class.getClassLoader().getResource(name.replace(File.separatorChar, '/'));
    if (url != null) {
      try {
        return new File(url.toURI());
      } catch (Exception e) {
        throw new RuntimeException(
            "Resource was found on classpath, but cannot be resolved to a "
                + "normal file (maybe it is part of a JAR file): "
                + name);
      }
    }
    File file = new File(name);
    if (file.exists()) {
      return file;
    } else {
      file = new File("../../../", name);
      if (file.exists()) {
        return file;
      }
    }
    throw new RuntimeException(
        "Cannot find resource in classpath or in file-system (relative to CWD): "
            + name
            + " CWD="
            + Paths.get("").toAbsolutePath());
  }
}

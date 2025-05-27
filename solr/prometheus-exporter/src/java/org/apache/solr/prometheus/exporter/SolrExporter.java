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
package org.apache.solr.prometheus.exporter;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.prometheus.collector.MetricsCollectorFactory;
import org.apache.solr.prometheus.collector.SchedulerMetricsCollector;
import org.apache.solr.prometheus.scraper.SolrCloudScraper;
import org.apache.solr.prometheus.scraper.SolrScraper;
import org.apache.solr.prometheus.scraper.SolrStandaloneScraper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrExporter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int DEFAULT_PORT = 8989;
  private static final String DEFAULT_BASE_URL = getDefaultSolrUrl();
  private static final String DEFAULT_ZK_HOST = "";
  private static final String DEFAULT_CONFIG = "solr-exporter-config.xml";
  private static final int DEFAULT_SCRAPE_INTERVAL = 60;
  private static final Integer DEFAULT_NUM_THREADS = 1;
  private static final String DEFAULT_CREDENTIALS = "";

  public static final CollectorRegistry defaultRegistry = new CollectorRegistry();

  private final int port;
  private final CachedPrometheusCollector prometheusCollector;
  private final SchedulerMetricsCollector metricsCollector;
  private final SolrScraper solrScraper;

  private final ExecutorService metricCollectorExecutor;
  private final ExecutorService requestExecutor;

  private HTTPServer httpServer;

  public SolrExporter(
      int port,
      int numberThreads,
      int scrapeInterval,
      SolrScrapeConfiguration scrapeConfiguration,
      MetricsConfiguration metricsConfiguration,
      String clusterId) {
    this.port = port;

    this.metricCollectorExecutor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            numberThreads, new SolrNamedThreadFactory("solr-exporter-collectors"));

    this.requestExecutor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            numberThreads, new SolrNamedThreadFactory("solr-exporter-requests"));

    this.solrScraper =
        createScraper(scrapeConfiguration, metricsConfiguration.getSettings(), clusterId);
    this.metricsCollector =
        new MetricsCollectorFactory(
                metricCollectorExecutor, scrapeInterval, solrScraper, metricsConfiguration)
            .create();
    this.prometheusCollector = new CachedPrometheusCollector();
  }

  void start() throws IOException {
    defaultRegistry.register(prometheusCollector);

    metricsCollector.addObserver(prometheusCollector);
    metricsCollector.start();

    httpServer = new HTTPServer(new InetSocketAddress(port), defaultRegistry);
  }

  void stop() {
    if (httpServer != null) {
      httpServer.stop();
    }

    metricsCollector.removeObserver(prometheusCollector);

    requestExecutor.shutdownNow();
    metricCollectorExecutor.shutdownNow();

    IOUtils.closeQuietly(metricsCollector);
    IOUtils.closeQuietly(solrScraper);

    defaultRegistry.unregister(this.prometheusCollector);
  }

  private SolrScraper createScraper(
      SolrScrapeConfiguration configuration,
      PrometheusExporterSettings settings,
      String clusterId) {
    SolrClientFactory factory = new SolrClientFactory(settings, configuration);

    switch (configuration.getType()) {
      case STANDALONE:
        return new SolrStandaloneScraper(
            factory.createStandaloneSolrClient(configuration.getSolrHost().get()),
            requestExecutor,
            clusterId);
      case CLOUD:
        return new SolrCloudScraper(
            factory.createCloudSolrClient(configuration.getZookeeperConnectionString().get()),
            requestExecutor,
            factory,
            clusterId);
      default:
        throw new RuntimeException("Invalid type: " + configuration.getType());
    }
  }

  public static void main(String[] args) {
    Options mainOptions = new Options();
    Options deprecatedOptions = new Options();

    // Change to -s and --solr-url in main once deprecated -s flag for --scrape-interval is removed.
    Option baseUrlOption =
        Option.builder("b")
            .longOpt("base-url")
            .hasArg()
            .argName("BASE_URL")
            .type(String.class)
            .desc(
                "Specify the Solr base URL when connecting to Solr in standalone mode. If omitted both the -b parameter and the -z parameter, connect to "
                    + DEFAULT_BASE_URL
                    + ".")
            .build();
    mainOptions.addOption(baseUrlOption);

    Option baseUrlDepOption =
        Option.builder()
            .longOpt("baseUrl")
            .hasArg()
            .argName("BASE_URL")
            .type(String.class)
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --base-url instead")
                    .get())
            .desc(
                "Specify the Solr base URL when connecting to Solr in standalone mode. If omitted both the -b parameter and the -z parameter, connect to http://localhost:8983/solr. For example 'http://localhost:8983/solr'.")
            .build();
    deprecatedOptions.addOption(baseUrlDepOption);

    Option configOption =
        Option.builder()
            .longOpt("config-file")
            .hasArg()
            .argName("CONFIG")
            .type(String.class)
            .desc("Specify the configuration file; the default is " + DEFAULT_CONFIG + ".")
            .build();
    Option configOptionDeprecated =
        Option.builder("f")
            .hasArg()
            .argName("CONFIG")
            .type(String.class)
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.8")
                    .setDescription("Use --config-file instead")
                    .get())
            .desc("Specify the configuration file; the default is " + DEFAULT_CONFIG + ".")
            .build();
    mainOptions.addOption(configOption);
    mainOptions.addOption(configOptionDeprecated);

    Option helpOption =
        Option.builder("h").longOpt("help").desc("Prints this help message.").build();
    mainOptions.addOption(helpOption);

    Option clusterIdOption =
        Option.builder()
            .longOpt("cluster-id")
            .hasArg()
            .argName("CLUSTER_ID")
            .type(String.class)
            .desc(
                "Specify a unique identifier for the cluster, which can be used to select between multiple clusters in Grafana. By default this ID will be equal to a hash of the -b or -z argument")
            .build();
    mainOptions.addOption(clusterIdOption);

    Option clusterIdDepOption =
        Option.builder("i")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.8")
                    .setDescription("Use --cluster-id instead")
                    .get())
            .hasArg()
            .argName("CLUSTER_ID")
            .type(String.class)
            .desc(
                "Specify a unique identifier for the cluster, which can be used to select between multiple clusters in Grafana. By default this ID will be equal to a hash of the -b or -z argument")
            .build();
    deprecatedOptions.addOption(clusterIdDepOption);

    Option numThreadsOption =
        Option.builder()
            .longOpt("num-threads")
            .hasArg()
            .argName("NUM_THREADS")
            .type(Integer.class)
            .desc(
                "Specify the number of threads. solr-exporter creates a thread pools for request to Solr. If you need to improve request latency via solr-exporter, you can increase the number of threads; the default is "
                    + DEFAULT_NUM_THREADS
                    + ".")
            .build();
    mainOptions.addOption(numThreadsOption);
    Option numThreadsOptionDeprecated =
        Option.builder("n")
            .hasArg()
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.8")
                    .setDescription("Use --num-threads instead")
                    .get())
            .argName("NUM_THREADS")
            .type(Integer.class)
            .desc(
                "Specify the number of threads. solr-exporter creates a thread pools for request to Solr. If you need to improve request latency via solr-exporter, you can increase the number of threads; the default is "
                    + DEFAULT_NUM_THREADS
                    + ".")
            .build();
    mainOptions.addOption(numThreadsOptionDeprecated);

    Option portOption =
        Option.builder("p")
            .longOpt("port")
            .hasArg()
            .argName("PORT")
            .type(Integer.class)
            .desc("Specify the solr-exporter HTTP listen port; default is " + DEFAULT_PORT + ".")
            .build();
    mainOptions.addOption(portOption);

    Option scrapeIntervalOption =
        Option.builder()
            .longOpt("scrape-interval")
            .hasArg()
            .argName("SCRAPE_INTERVAL")
            .type(Integer.class)
            .desc(
                "Specify the delay between scraping Solr metrics; the default is "
                    + DEFAULT_SCRAPE_INTERVAL
                    + " seconds.")
            .build();
    mainOptions.addOption(scrapeIntervalOption);

    Option scrapeIntervalOptionDeprecated =
        Option.builder("s")
            .hasArg()
            .argName("SCRAPE_INTERVAL")
            .type(Integer.class)
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.8")
                    .setDescription("Use --scrape-interval instead")
                    .get())
            .desc(
                "Specify the delay between scraping Solr metrics; the default is "
                    + DEFAULT_SCRAPE_INTERVAL
                    + " seconds.")
            .build();
    mainOptions.addOption(scrapeIntervalOptionDeprecated);

    Option sslOption =
        Option.builder("ssl")
            .longOpt("ssl-enabled")
            .type(Boolean.class)
            .desc(
                "Enable TLS connection to Solr. Expects following env variables: SOLR_SSL_KEY_STORE, SOLR_SSL_KEY_STORE_PASSWORD, SOLR_SSL_TRUST_STORE, SOLR_SSL_TRUST_STORE_PASSWORD. Example: --ssl-enabled")
            .build();
    mainOptions.addOption(sslOption);

    Option credentialsOption =
        Option.builder("u")
            .longOpt("credentials")
            .hasArg()
            .argName("CREDENTIALS")
            .type(String.class)
            .desc(
                "Specify the credentials in the format username:password. Example: --credentials solr:SolrRocks")
            .build();
    mainOptions.addOption(credentialsOption);

    Option zkHostOption =
        Option.builder("z")
            .longOpt("zk-host")
            .hasArg()
            .argName("ZK_HOST")
            .type(String.class)
            .desc(
                "Specify the ZooKeeper connection string when connecting to Solr in SolrCloud mode. If omitted both the -b parameter and the -z parameter, connect to "
                    + DEFAULT_BASE_URL
                    + ".")
            .build();
    mainOptions.addOption(zkHostOption);

    Options options = new Options();
    options.addOptions(mainOptions);
    options.addOptions(deprecatedOptions);

    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine commandLine = parser.parse(options, args);

      if (commandLine.hasOption(helpOption)) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
            "bin/solr-exporter", "Prometheus exporter for Apache Solr.", mainOptions, null, true);
        return;
      }

      final SolrScrapeConfiguration scrapeConfiguration;
      final String defaultClusterId;
      if (commandLine.hasOption(zkHostOption)) {
        String zkHost = commandLine.getOptionValue(zkHostOption, DEFAULT_ZK_HOST);
        defaultClusterId = makeShortHash(zkHost);
        scrapeConfiguration = SolrScrapeConfiguration.solrCloud(zkHost);
      } else if (commandLine.hasOption(baseUrlOption) || commandLine.hasOption(baseUrlDepOption)) {
        log.warn("-b and --base-url will be replaced with -s and --solr-url in Solr 10");
        String baseUrl =
            commandLine.hasOption(baseUrlOption)
                ? commandLine.getOptionValue(baseUrlOption)
                : commandLine.getOptionValue(baseUrlDepOption, DEFAULT_BASE_URL);
        defaultClusterId = makeShortHash(baseUrl);
        scrapeConfiguration = SolrScrapeConfiguration.standalone(baseUrl);
      } else {
        String baseUrl = DEFAULT_BASE_URL;
        if (log.isInfoEnabled()) {
          log.info(
              "Neither --{} or --{} parameters provided so assuming solr url is {}",
              baseUrlOption.getLongOpt(),
              zkHostOption.getLongOpt(),
              baseUrl);
        }
        defaultClusterId = makeShortHash(baseUrl);
        scrapeConfiguration = SolrScrapeConfiguration.standalone(baseUrl);
      }

      int port = commandLine.getParsedOptionValue(portOption, DEFAULT_PORT);
      String clusterId = commandLine.getOptionValue(clusterIdOption, defaultClusterId);
      if (commandLine.hasOption("i")) {
        clusterId = commandLine.getOptionValue("i");
      }
      if (StrUtils.isNullOrEmpty(clusterId)) {
        clusterId = defaultClusterId;
      }

      if (commandLine.hasOption(credentialsOption)) {
        String credentials = commandLine.getOptionValue(credentialsOption, DEFAULT_CREDENTIALS);
        if (credentials.indexOf(':') > 0) {
          String[] credentialsArray = credentials.split(":", 2);
          scrapeConfiguration.withBasicAuthCredentials(credentialsArray[0], credentialsArray[1]);
        }
      }

      if (commandLine.hasOption(sslOption)) {
        log.info("SSL ENABLED");

        scrapeConfiguration.withSslConfiguration(
            Paths.get(getSystemVariable("SOLR_SSL_KEY_STORE")),
            getSystemVariable("SOLR_SSL_KEY_STORE_PASSWORD"),
            Paths.get(getSystemVariable("SOLR_SSL_TRUST_STORE")),
            getSystemVariable("SOLR_SSL_TRUST_STORE_PASSWORD"));
      }

      String configFile = DEFAULT_CONFIG;
      if (commandLine.hasOption(configOptionDeprecated)) {
        configFile = commandLine.getOptionValue(configOptionDeprecated);
      } else if (commandLine.hasOption(configOption)) {
        configFile = commandLine.getOptionValue(configOption);
      }
      int numberOfThreads = DEFAULT_NUM_THREADS;
      if (commandLine.hasOption("num-threads")) {
        numberOfThreads = commandLine.getParsedOptionValue("num-threads");
      } else if (commandLine.hasOption("n")) {
        numberOfThreads = commandLine.getParsedOptionValue("n");
      }

      int scrapeInterval = DEFAULT_SCRAPE_INTERVAL;
      if (commandLine.hasOption("s")) {
        scrapeInterval = commandLine.getParsedOptionValue(scrapeIntervalOptionDeprecated);
      } else if (commandLine.hasOption("scrape-interval")) {
        scrapeInterval = commandLine.getParsedOptionValue(scrapeIntervalOption);
      }

      SolrExporter solrExporter =
          new SolrExporter(
              port,
              numberOfThreads,
              scrapeInterval,
              scrapeConfiguration,
              loadMetricsConfiguration(configFile),
              clusterId);

      log.info("Starting Solr Prometheus Exporting on port {}", port);
      solrExporter.start();
      log.info(
          "Solr Prometheus Exporter is running. Collecting metrics for cluster {}: {}",
          clusterId,
          scrapeConfiguration);
    } catch (IOException e) {
      exit(1, "Failed to start Solr Prometheus Exporter: " + e.getMessage());
    } catch (ParseException e) {
      exit(1, "Failed to parse command line arguments: " + e.getMessage());
    }
  }

  /**
   * Creates a short 10-char hash of a longer string, based on first chars of the sha256 hash
   *
   * @param inputString original string
   * @return 10 char hash
   */
  static String makeShortHash(String inputString) {
    return DigestUtils.sha256Hex(inputString).substring(0, 10);
  }

  private static MetricsConfiguration loadMetricsConfiguration(String configPath) {
    try {
      return MetricsConfiguration.from(configPath);
    } catch (Exception e) {
      log.error("Could not load scrape configuration from {}", configPath);
      throw new RuntimeException(e);
    }
  }

  private static String getSystemVariable(String name) {
    return System.getProperty(name, System.getenv(name));
  }

  @SuppressForbidden(reason = "For use in command line tools only")
  public static void exit(int exitStatus, String message) {
    System.err.println(message);
    try {
      System.exit(exitStatus);
    } catch (java.lang.SecurityException secExc) {
      if (exitStatus != 0)
        throw new RuntimeException("SolrExporter failed to exit with status " + exitStatus);
    }
  }

  // copied over from CLIUtils
  private static String getDefaultSolrUrl() {
    // note that ENV_VAR syntax (and the env vars too) are mapped to env.var sys props
    String scheme = EnvUtils.getProperty("solr.url.scheme", "http");
    String host = EnvUtils.getProperty("solr.tool.host", "localhost");
    String port = EnvUtils.getProperty("jetty.port", "8983"); // from SOLR_PORT env
    return String.format(Locale.ROOT, "%s://%s:%s", scheme.toLowerCase(Locale.ROOT), host, port);
  }
}

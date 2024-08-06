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
package org.apache.solr.cloud;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.VALUE_LONG;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.cli.CLIO;
import org.apache.solr.cli.SolrCLI;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.util.Compressor;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.ZLibCompressor;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.xml.sax.SAXException;

public class ZkCLI implements CLIO {

  private static final String MAKEPATH = "makepath";
  private static final String PUT = "put";
  private static final String PUT_FILE = "putfile";
  private static final String GET = "get";
  private static final String GET_FILE = "getfile";
  private static final String DOWNCONFIG = "downconfig";
  private static final String ZK_CLI_NAME = "ZkCLI";

  /**
   * @deprecated Replaced by LINK_CONFIG.
   */
  @Deprecated(since = "9.7")
  private static final String LINKCONFIG = "linkconfig";

  private static final String LINK_CONFIG = "link-config";

  /**
   * @deprecated Replaced by CONF_DIR.
   */
  @Deprecated(since = "9.7")
  private static final String CONFDIR = "confdir";

  private static final String CONF_DIR = "conf-dir";

  /**
   * @deprecated Replaced by CONF_NAME.
   */
  @Deprecated(since = "9.7")
  private static final String CONFNAME = "confname";

  private static final String CONF_NAME = "conf-name";

  /**
   * @deprecated Replaced by ZK_HOST.
   */
  @Deprecated(since = "9.7")
  private static final String ZKHOST = "zkhost";

  private static final String ZK_HOST = "zk-host";

  /**
   * @deprecated Replaced by RUN_ZK.
   */
  @Deprecated(since = "9.7")
  private static final String RUNZK = "runzk";

  private static final String RUN_ZK = "run-zk";

  /**
   * @deprecated Replaced by SOLR_HOME.
   */
  @Deprecated(since = "9.7")
  private static final String SOLRHOME = "solrhome";

  private static final String SOLR_HOME = "solr-home";
  private static final String BOOTSTRAP = "bootstrap";
  static final String UPCONFIG = "upconfig";
  static final String EXCLUDE_REGEX_SHORT = "x";

  /**
   * @deprecated Replaced by EXCLUDE_REGEX.
   */
  @Deprecated(since = "9.7")
  static final String EXCLUDEREGEX = "excluderegex";

  static final String EXCLUDE_REGEX = "exclude-regex";
  static final String EXCLUDE_REGEX_DEFAULT = ConfigSetService.UPLOAD_FILENAME_EXCLUDE_REGEX;
  private static final String COLLECTION = "collection";
  private static final String CLEAR = "clear";
  private static final String LIST = "list";
  private static final String LS = "ls";
  private static final String CMD = "cmd";
  private static final String CLUSTERPROP = "clusterprop";
  private static final String UPDATEACLS = "updateacls";

  @VisibleForTesting
  public static void setStdout(PrintStream stdout) {
    ZkCLI.stdout = stdout;
  }

  private static PrintStream stdout = CLIO.getOutStream();

  /**
   * Allows you to perform a variety of zookeeper related tasks, such as:
   *
   * <p>Bootstrap the current configs for all collections in solr.xml.
   *
   * <p>Upload a named config set from a given directory.
   *
   * <p>Link a named config set explicity to a collection.
   *
   * <p>Clear ZooKeeper info.
   *
   * <p>If you also pass a solrPort, it will be used to start an embedded zk useful for single
   * machine, multi node tests.
   */
  public static void main(String[] args)
      throws InterruptedException,
          TimeoutException,
          IOException,
          ParserConfigurationException,
          SAXException,
          KeeperException {

    CommandLineParser parser = new PosixParser();
    Options options = new Options();

    Option cmdOption =
        Option.builder(CMD)
            .hasArg(true)
            .desc(
                "cmd to run: "
                    + BOOTSTRAP
                    + ", "
                    + UPCONFIG
                    + ", "
                    + DOWNCONFIG
                    + ", "
                    + LINKCONFIG
                    + ", "
                    + MAKEPATH
                    + ", "
                    + PUT
                    + ", "
                    + PUT_FILE
                    + ","
                    + GET
                    + ","
                    + GET_FILE
                    + ", "
                    + LIST
                    + ", "
                    + CLEAR
                    + ", "
                    + UPDATEACLS
                    + ", "
                    + LS)
            .build();
    options.addOption(cmdOption);

    Option zkHostDepOption =
        Option.builder(ZKHOST)
            .argName("zkHost")
            .hasArg()
            .desc("ZooKeeper host address")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --zk-host instead")
                    .get())
            .build();
    options.addOption(zkHostDepOption);

    Option zkHostOption =
        Option.builder("z")
            .longOpt(ZK_HOST)
            .argName("zkHost")
            .hasArg()
            .desc("ZooKeeper host address")
            .build();
    options.addOption(zkHostOption);

    Option solrHomeDepOption =
        Option.builder(SOLRHOME)
            .hasArg()
            .desc("for " + BOOTSTRAP + ", " + RUNZK + ": solrhome location")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --solr-home instead")
                    .get())
            .build();
    options.addOption(solrHomeDepOption);

    Option solrHomeOption =
        Option.builder("s")
            .longOpt(SOLR_HOME)
            .hasArg()
            .desc("for " + BOOTSTRAP + ", " + RUNZK + ": solr-home location")
            .build();
    options.addOption(solrHomeOption);

    Option confDirDepOption =
        Option.builder(CONFDIR)
            .hasArg()
            .argName("DIR")
            .desc("for " + UPCONFIG + ": a directory of configuration files")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --conf-dir instead")
                    .get())
            .build();
    options.addOption(confDirDepOption);

    Option confDirOption =
        Option.builder("d")
            .longOpt(CONF_DIR)
            .hasArg()
            .argName("DIR")
            .desc("for " + UPCONFIG + ": a directory of configuration files")
            .build();
    options.addOption(confDirOption);

    Option confNameDepOption =
        Option.builder(CONFNAME)
            .hasArg()
            .argName("NAME")
            .desc("for " + UPCONFIG + ", " + LINKCONFIG + ": name of the config set")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --conf-name instead")
                    .get())
            .build();
    options.addOption(confNameDepOption);

    Option confNameOption =
        Option.builder("n")
            .longOpt(CONF_NAME)
            .hasArg()
            .argName("NAME")
            .desc("for " + UPCONFIG + ", " + LINKCONFIG + ": name of the config set")
            .build();
    options.addOption(confNameOption);

    Option collectionOption =
        Option.builder("c")
            .longOpt(COLLECTION)
            .hasArg()
            .argName("NAME")
            .desc("for " + LINKCONFIG + ": name of the collection")
            .build();
    options.addOption(collectionOption);

    Option excludeRegexDepOption =
        Option.builder(EXCLUDEREGEX)
            .hasArg()
            .argName("<true/false>")
            .desc("for " + UPCONFIG + ": files matching this regular expression won't be uploaded")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --exclude-regex instead")
                    .get())
            .build();
    options.addOption(excludeRegexDepOption);

    Option excludeRegexOption =
        Option.builder(EXCLUDE_REGEX_SHORT)
            .longOpt(EXCLUDE_REGEX)
            .hasArg()
            .argName("<true/false>")
            .desc("for " + UPCONFIG + ": files matching this regular expression won't be uploaded")
            .build();
    options.addOption(excludeRegexOption);

    Option runZkDepOption =
        Option.builder(RUNZK)
            .hasArg()
            .argName("<true/false>")
            .desc(
                "run zk internally by passing the solr run port - only for clusters on one machine (tests, dev)")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --run-zk instead")
                    .get())
            .build();
    options.addOption(runZkDepOption);

    Option runZkOption =
        Option.builder("r")
            .longOpt(RUN_ZK)
            .hasArg()
            .argName("<true/false>")
            .desc(
                "run zk internally by passing the solr run port - only for clusters on one machine (tests, dev)")
            .build();
    options.addOption(runZkOption);

    Option clusterNameOption =
        Option.builder(NAME).hasArg().desc("name of the cluster property to set").build();
    options.addOption(clusterNameOption);

    Option clusterValueOption =
        Option.builder(VALUE_LONG).hasArg().desc("value of the cluster to set").build();
    options.addOption(clusterValueOption);

    options.addOption(SolrCLI.OPTION_HELP);
    options.addOption(SolrCLI.OPTION_VERBOSE);

    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      if ((line.hasOption(SolrCLI.OPTION_HELP)
              || (!line.hasOption(ZK_HOST) && !line.hasOption(ZKHOST))
              || !line.hasOption(CMD))
          && !line.hasOption(SolrCLI.OPTION_VERBOSE)) {
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(ZK_CLI_NAME, options);
        stdout.println("Examples:");
        stdout.println(
            "zkcli.sh --zk-host localhost:9983 -cmd "
                + BOOTSTRAP
                + " -"
                + SOLR_HOME
                + " /opt/solr");
        stdout.println(
            "zkcli.sh --zk-host localhost:9983 -cmd "
                + UPCONFIG
                + " -"
                + CONF_DIR
                + " /opt/solr/collection1/conf"
                + " -"
                + CONF_NAME
                + " myconf");
        stdout.println(
            "zkcli.sh --zk-host localhost:9983 -cmd "
                + DOWNCONFIG
                + " -"
                + CONF_DIR
                + " /opt/solr/collection1/conf"
                + " -"
                + CONF_NAME
                + " myconf");
        stdout.println(
            "zkcli.sh --zk-host localhost:9983 -cmd "
                + LINKCONFIG
                + " -"
                + COLLECTION
                + " collection1"
                + " -"
                + CONF_NAME
                + " myconf");
        stdout.println("zkcli.sh --zk-host localhost:9983 -cmd " + MAKEPATH + " /apache/solr");
        stdout.println("zkcli.sh --zk-host localhost:9983 -cmd " + PUT + " /solr.conf 'conf data'");
        stdout.println(
            "zkcli.sh --zk-host localhost:9983 -cmd "
                + PUT_FILE
                + " /solr.xml /User/myuser/solr/solr.xml");
        stdout.println("zkcli.sh --zk-host localhost:9983 -cmd " + GET + " /solr.xml");
        stdout.println(
            "zkcli.sh --zk-host localhost:9983 -cmd " + GET_FILE + " /solr.xml solr.xml.file");
        stdout.println("zkcli.sh --zk-host localhost:9983 -cmd " + CLEAR + " /solr");
        stdout.println("zkcli.sh --zk-host localhost:9983 -cmd " + LIST);
        stdout.println("zkcli.sh --zk-host localhost:9983 -cmd " + LS + " /solr/live_nodes");
        stdout.println(
            "zkcli.sh --zk-host localhost:9983 -cmd "
                + CLUSTERPROP
                + " -"
                + NAME
                + " urlScheme -"
                + VALUE_LONG
                + " https");
        stdout.println("zkcli.sh --zk-host localhost:9983 -cmd " + UPDATEACLS + " /solr");
        return;
      }

      // start up a tmp zk server first
      String zkServerAddress =
          line.hasOption(ZK_HOST) ? line.getOptionValue(ZK_HOST) : line.getOptionValue(ZKHOST);
      String solrHome =
          line.hasOption(SOLR_HOME)
              ? line.getOptionValue(SOLR_HOME)
              : line.getOptionValue(SOLRHOME);
      if (StrUtils.isNullOrEmpty(solrHome)) {
        solrHome = System.getProperty("solr.home");
      }
      if (line.hasOption(SolrCLI.OPTION_VERBOSE)) {
        stdout.println("Using " + SOLR_HOME + "=" + solrHome);
        return;
      }

      String solrPort = null;
      if (line.hasOption(RUNZK) || line.hasOption(RUN_ZK)) {
        if (!line.hasOption(SOLR_HOME) && !line.hasOption(SOLRHOME)) {
          stdout.println("--" + SOLR_HOME + " is required for " + RUN_ZK);
          System.exit(1);
        }
        solrPort =
            line.hasOption(RUN_ZK) ? line.getOptionValue(RUN_ZK) : line.getOptionValue(RUNZK);
      }

      SolrZkServer zkServer = null;
      if (solrPort != null) {
        zkServer =
            new SolrZkServer(
                "true",
                null,
                new File(solrHome, "/zoo_data"),
                solrHome,
                Integer.parseInt(solrPort));
        zkServer.parseConfig();
        zkServer.start();
      }

      int minStateByteLenForCompression = -1;
      Compressor compressor = new ZLibCompressor();

      if (solrHome != null) {
        try {
          Path solrHomePath = Paths.get(solrHome);
          Properties props = new Properties();
          props.put(SolrXmlConfig.ZK_HOST, zkServerAddress);
          NodeConfig nodeConfig = NodeConfig.loadNodeConfig(solrHomePath, props);
          minStateByteLenForCompression =
              nodeConfig.getCloudConfig().getMinStateByteLenForCompression();
          String stateCompressorClass = nodeConfig.getCloudConfig().getStateCompressorClass();
          if (StrUtils.isNotNullOrEmpty(stateCompressorClass)) {
            Class<? extends Compressor> compressionClass =
                Class.forName(stateCompressorClass).asSubclass(Compressor.class);
            compressor = compressionClass.getDeclaredConstructor().newInstance();
          }
        } catch (SolrException e) {
          // Failed to load solr.xml
          stdout.println(
              "Failed to load solr.xml from ZK or SolrHome, put/get operations on compressed data will use data as is. If you intention is to read and de-compress data or compress and write data, then solr.xml must be accessible.");
        } catch (ClassNotFoundException
            | NoSuchMethodException
            | InstantiationException
            | IllegalAccessException
            | InvocationTargetException e) {
          stdout.println("Unable to find or instantiate compression class: " + e.getMessage());
          System.exit(1);
        }
      }

      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(zkServerAddress)
              .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
              .withConnTimeOut(
                  SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS)
              .withReconnectListener(() -> {})
              // .withCompressor(compressor)
              .withStateFileCompression(minStateByteLenForCompression, compressor)
              .build()) {
        if (line.getOptionValue(CMD).equalsIgnoreCase(BOOTSTRAP)) {
          if (!line.hasOption(SOLR_HOME) && !line.hasOption(SOLRHOME)) {
            stdout.println("--" + SOLR_HOME + " is required for " + BOOTSTRAP);
            System.exit(1);
          }

          CoreContainer cc = new CoreContainer(Paths.get(solrHome), new Properties());
          cc.setCoreConfigService(new ZkConfigSetService(zkClient));

          if (!ZkController.checkChrootPath(zkServerAddress, true)) {
            stdout.println("A chroot was specified in zkHost but the znode doesn't exist. ");
            System.exit(1);
          }

          ConfigSetService.bootstrapConf(cc);

          // No need to close the CoreContainer, as it wasn't started
          // up in the first place...

        } else if (line.getOptionValue(CMD).equalsIgnoreCase(UPCONFIG)) {
          if ((!line.hasOption(CONF_DIR) && !line.hasOption(CONFDIR))
              || (!line.hasOption(CONF_NAME) && !line.hasOption(CONFNAME))) {
            stdout.println(
                "--" + CONF_DIR + " and --" + CONF_NAME + " are required for " + UPCONFIG);
            System.exit(1);
          }
          String confDir =
              line.hasOption(CONF_DIR)
                  ? line.getOptionValue(CONF_DIR)
                  : line.getOptionValue(CONFDIR);
          String confName =
              line.hasOption(CONF_NAME)
                  ? line.getOptionValue(CONF_NAME)
                  : line.getOptionValue(CONFNAME);
          final String excludeExpr =
              line.hasOption(EXCLUDE_REGEX)
                  ? line.getOptionValue(EXCLUDE_REGEX, EXCLUDE_REGEX_DEFAULT)
                  : line.getOptionValue(EXCLUDEREGEX, EXCLUDE_REGEX_DEFAULT);

          if (!ZkController.checkChrootPath(zkServerAddress, true)) {
            stdout.println("A chroot was specified in zkHost but the znode doesn't exist. ");
            System.exit(1);
          }
          final Pattern excludePattern = Pattern.compile(excludeExpr);
          ZkMaintenanceUtils.uploadToZK(
              zkClient,
              Paths.get(confDir),
              ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confName,
              excludePattern);
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(DOWNCONFIG)) {
          if ((!line.hasOption(CONF_DIR) && !line.hasOption(CONFDIR))
              || (!line.hasOption(CONF_NAME) && !line.hasOption(CONFNAME))) {
            stdout.println(
                "--" + CONF_DIR + " and -" + CONF_NAME + " are required for " + DOWNCONFIG);
            System.exit(1);
          }
          String confDir =
              line.hasOption(CONF_DIR)
                  ? line.getOptionValue(CONF_DIR)
                  : line.getOptionValue(CONFDIR);
          String confName =
              line.hasOption(CONF_NAME)
                  ? line.getOptionValue(CONF_NAME)
                  : line.getOptionValue(CONFNAME);
          ZkMaintenanceUtils.downloadFromZK(
              zkClient, ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confName, Paths.get(confDir));
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(LINKCONFIG)) {
          if (!line.hasOption(COLLECTION)
              || (!line.hasOption(CONF_NAME) && !line.hasOption(CONFNAME))) {
            stdout.println(
                "--" + COLLECTION + " and --" + CONF_NAME + " are required for " + LINKCONFIG);
            System.exit(1);
          }
          String collection = line.getOptionValue(COLLECTION);
          String confName =
              line.hasOption(CONF_NAME)
                  ? line.getOptionValue(CONF_NAME)
                  : line.getOptionValue(CONFNAME);

          ZkController.linkConfSet(zkClient, collection, confName);
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(LIST)) {
          zkClient.printLayoutToStream(stdout);
        } else if (line.getOptionValue(CMD).equals(LS)) {

          List<String> argList = line.getArgList();
          if (argList.size() != 1) {
            stdout.println("-" + LS + " requires one arg - the path to list");
            System.exit(1);
          }

          StringBuilder sb = new StringBuilder();
          String path = argList.get(0);
          zkClient.printLayout(path == null ? "/" : path, 0, sb);
          stdout.println(sb);

        } else if (line.getOptionValue(CMD).equalsIgnoreCase(CLEAR)) {
          List<String> arglist = line.getArgList();
          if (arglist.size() != 1) {
            stdout.println("-" + CLEAR + " requires one arg - the path to clear");
            System.exit(1);
          }
          zkClient.clean(arglist.get(0));
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(MAKEPATH)) {
          List<String> arglist = line.getArgList();
          if (arglist.size() != 1) {
            stdout.println("-" + MAKEPATH + " requires one arg - the path to make");
            System.exit(1);
          }
          if (!ZkController.checkChrootPath(zkServerAddress, true)) {
            stdout.println("A chroot was specified in zkHost but the znode doesn't exist. ");
            System.exit(1);
          }
          zkClient.makePath(arglist.get(0), true);
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(PUT)) {
          List<String> arglist = line.getArgList();
          if (arglist.size() != 2) {
            stdout.println(
                "-" + PUT + " requires two args - the path to create and the data string");
            System.exit(1);
          }
          String path = arglist.get(0);
          byte[] data = arglist.get(1).getBytes(StandardCharsets.UTF_8);
          if (shouldCompressData(data, path, minStateByteLenForCompression)) {
            // state.json should be compressed before being put to ZK
            // data = compressor.compressBytes(data, data.length / 10);
          }
          if (zkClient.exists(path, true)) {
            zkClient.setData(path, data, true);
          } else {
            zkClient.makePath(path, data, CreateMode.PERSISTENT, true);
          }
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(PUT_FILE)) {
          List<String> arglist = line.getArgList();
          if (arglist.size() != 2) {
            stdout.println(
                "-"
                    + PUT_FILE
                    + " requires two args - the path to create in ZK and the path to the local file");
            System.exit(1);
          }

          String path = arglist.get(0);
          byte[] data = Files.readAllBytes(Path.of(arglist.get(1)));
          if (shouldCompressData(data, path, minStateByteLenForCompression)) {
            // state.json should be compressed before being put to ZK
            // data = compressor.compressBytes(data, data.length / 10);
          }
          if (zkClient.exists(path, true)) {
            zkClient.setData(path, data, true);
          } else {
            if (!ZkController.checkChrootPath(zkServerAddress, true)) {
              stdout.println("A chroot was specified in zkHost but the znode doesn't exist. ");
              System.exit(1);
            }
            zkClient.makePath(path, data, CreateMode.PERSISTENT, true);
          }
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(GET)) {
          List<String> arglist = line.getArgList();
          if (arglist.size() != 1) {
            stdout.println("-" + GET + " requires one arg - the path to get");
            System.exit(1);
          }
          byte[] data = zkClient.getData(arglist.get(0), null, null, true);
          stdout.println(new String(data, StandardCharsets.UTF_8));
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(GET_FILE)) {
          List<String> arglist = line.getArgList();
          if (arglist.size() != 2) {
            stdout.println(
                "-" + GET_FILE + "requires two args - the path to get and the file to save it to");
            System.exit(1);
          }
          byte[] data = zkClient.getData(arglist.get(0), null, null, true);
          Files.write(Path.of(arglist.get(1)), data);
        } else if (line.getOptionValue(CMD).equals(UPDATEACLS)) {
          List<String> arglist = line.getArgList();
          if (arglist.size() != 1) {
            stdout.println("-" + UPDATEACLS + " requires one arg - the path to update");
            System.exit(1);
          }
          zkClient.updateACLs(arglist.get(0));
        } else if (line.getOptionValue(CMD).equalsIgnoreCase(CLUSTERPROP)) {
          if (!line.hasOption(NAME)) {
            stdout.println("-" + NAME + " is required for " + CLUSTERPROP);
          }
          if (!ZkController.checkChrootPath(zkServerAddress, true)) {
            stdout.println("A chroot was specified in zkHost but the znode doesn't exist. ");
            System.exit(1);
          }
          String propertyName = line.getOptionValue(NAME);
          // If -val option is missing, we will use the null value. This is required to maintain
          // compatibility with Collections API.
          String propertyValue = line.getOptionValue(VALUE_LONG);
          ClusterProperties props = new ClusterProperties(zkClient);
          try {
            props.setClusterProperty(propertyName, propertyValue);
          } catch (IOException ex) {
            stdout.println(
                "Unable to set the cluster property due to following error : "
                    + ex.getLocalizedMessage());
            System.exit(1);
          }
        } else {
          // If not cmd matches
          stdout.println("Unknown command " + line.getOptionValue(CMD) + ". Use -h to get help.");
          System.exit(1);
        }
      } finally {
        if (solrPort != null) {
          zkServer.stop();
        }
      }
    } catch (ParseException exp) {
      stdout.println("Unexpected exception:" + exp.getMessage());
    }
  }

  private static boolean shouldCompressData(
      byte[] data, String path, int minStateByteLenForCompression) {
    if (path.endsWith("state.json")
        && minStateByteLenForCompression > -1
        && data.length > minStateByteLenForCompression) {
      // state.json should be compressed before being put to ZK
      return true;
    }
    return false;
  }
}

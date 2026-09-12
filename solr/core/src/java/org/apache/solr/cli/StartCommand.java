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

import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = "start",
    description = "Starts Solr",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Start Solr in SolrCloud mode on the default port",
      "  bin/solr start",
      "",
      "  # Start on a custom port with 2g heap",
      "  bin/solr start -p 8984 -m 2g",
      "",
      "  # Start in user managed mode",
      "  bin/solr start --user-managed"
    })
public class StartCommand implements Callable<Integer> {

  @CommandLine.Mixin HelpMixin helpMixin;

  @CommandLine.Spec CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(
      names = {"-f", "--foreground"},
      description =
          "Start Solr in foreground; default is background with logs to solr-PORT-console.log")
  boolean foreground;

  @CommandLine.Option(
      names = "--user-managed",
      description = "Start Solr in standalone mode. Default is SolrCloud (ZooKeeper) mode.")
  boolean userManaged;

  @CommandLine.Option(
      names = "--host",
      description =
          "Specify the hostname this Solr node will advertise to ZooKeeper and other cluster members (SOLR_HOST_ADVERTISE)."
              + " Defaults to the canonical hostname of the local host.")
  String host;

  @CommandLine.Option(
      names = {"-p", "--port"},
      description =
          "Specify the Solr HTTP port; default is 8983. STOP_PORT=($SOLR_PORT-1000), RMI_PORT=($SOLR_PORT+10000)")
  String port;

  @CommandLine.Option(
      names = "--server-dir",
      description = "Specify the Solr server directory; default is 'server'")
  String serverDir;

  @CommandLine.Option(
      names = {"-z", "--zk-host"},
      description =
          "Zookeeper connection string; default is to start an embedded ZooKeeper on PORT+10000")
  String zkHost;

  @CommandLine.Option(
      names = {"-m", "--memory"},
      description = "Set JVM heap size, e.g., -m 4g sets -Xms4g -Xmx4g; default is 512m")
  String memory;

  @CommandLine.Option(
      names = "--solr-home",
      description =
          "Set solr.solr.home system property; default is 'server/solr'. Ignored in examples mode")
  String solrHome;

  @CommandLine.Option(
      names = "--data-home",
      description =
          "Set solr.data.home system property for index data storage; default is solr.solr.home")
  String dataHome;

  @CommandLine.Option(
      names = {"-e", "--example"},
      description = "Run an example: cloud, techproducts, schemaless, films")
  String example;

  @CommandLine.Option(
      names = "--jvm-opts",
      description = "Additional JVM parameters, e.g., --jvm-opts \"-verbose:gc\"")
  String jvmOpts;

  @CommandLine.Option(
      names = {"-j", "--jettyconfig"},
      description =
          "Additional Jetty parameters, e.g., -j \"--include-jetty-dir=/etc/jetty/custom/server/\"")
  String jettyParams;

  @CommandLine.Option(
      names = {"-y", "--no-prompt"},
      description = "Don't prompt for input; accept all defaults when running examples")
  boolean noPrompt;

  @CommandLine.Option(
      names = "--prompt-inputs",
      description =
          "Don't prompt for input; comma-delimited list of inputs to use when running examples that accept user input",
      paramLabel = "<values>")
  String promptInputs;

  @CommandLine.Option(
      names = "--example-dir",
      description =
          "Override the directory containing example configurations used when running examples with --example")
  String exampleDir;

  @CommandLine.Option(
      names = "--force",
      description = "Override warning when attempting to start Solr as root user")
  boolean force;

  @CommandLine.Option(
      names = {"--verbose"},
      description = "Set log level to DEBUG (verbose); default is INFO")
  boolean verbose;

  @CommandLine.Option(
      names = {"--quiet", "-q"},
      description = "Set log level to WARN (quiet); default is INFO")
  boolean quiet;

  // TODO: SOLR-17697 remove --fullhelp when commons-cli is removed; picocli's --help already
  // produces equivalent output from the annotations.
  @CommandLine.Option(
      names = "--fullhelp",
      description = "Print detailed help with full option descriptions",
      hidden = true)
  boolean fullhelp;

  @Override
  public Integer call() {
    if (fullhelp) {
      printFullHelp();
    }
    // Actual start logic is handled by the bin/solr shell script.
    return 0;
  }

  // TODO: Remove when removing commons-cli
  private void printFullHelp() {
    CLIO.out(
        """
        Usage: solr start [OPTIONS]

        Starts Solr in standalone or SolrCloud mode.

        Options:
          -f, --foreground
              Start Solr in foreground; default starts Solr in the background and sends
              stdout / stderr to solr-PORT-console.log

          --user-managed
              Start Solr in user managed aka standalone mode.
              See: https://solr.apache.org/guide/solr/latest/deployment-guide/cluster-types.html

          --host <hostname>
              Specify the hostname for this Solr instance

          -p, --port <port>
              Specify the port to start the Solr HTTP listener on; default is 8983.
              The specified port (SOLR_PORT) will also be used to determine the stop port:
              STOP_PORT=($SOLR_PORT-1000) and JMX RMI listen port RMI_PORT=($SOLR_PORT+10000).
              For instance, if you set -p 8985, then STOP_PORT=7985 and RMI_PORT=18985

          --server-dir <dir>
              Specify the Solr server directory; defaults to server

          -z, --zk-host <zkHost>
              Zookeeper connection string; ignored in User Managed (--user-managed) mode.
              If neither ZK_HOST is defined in solr.in.sh nor -z is specified, an embedded
              ZooKeeper instance will be launched.
              Set ZK_CREATE_CHROOT=true if your ZK host has a chroot path to create it automatically.

          -m, --memory <size>
              Sets the min (-Xms) and max (-Xmx) heap size for the JVM, e.g., -m 4g sets
              -Xms4g -Xmx4g; by default, this script sets the heap size to 512m

          --solr-home <dir>
              Sets the solr.solr.home system property; Solr will create core directories here.
              Allows running multiple Solr instances on the same host while reusing the same
              server directory. If set, the directory should contain a solr.xml file unless
              solr.xml exists in ZooKeeper. Ignored when running examples (-e).
              Default: server/solr

          --data-home <dir>
              Sets the solr.data.home system property, where Solr stores index data in
              <instance_dir>/data subdirectories. If not set, Solr uses solr.solr.home.

          -e, --example <name>
              Name of the example to run; available examples:
                cloud:          SolrCloud example
                techproducts:   Comprehensive example illustrating many of Solr's core capabilities
                schemaless:     Schema-less example (schema inferred from data during indexing)
                films:          Example starting with _default configset with explicit fields

          --jvm-opts <jvmParams>
              Additional parameters to pass to the JVM when starting Solr, e.g., to enable
              a Java debugger: --jvm-opts "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=18983"
              Wrap additional parameters in double quotes.

          -j, --jettyconfig <jettyParams>
              Additional parameters to pass to Jetty, e.g., to add a config folder:
              -j "--include-jetty-dir=/etc/jetty/custom/server/"
              Wrap additional parameters in double quotes.

          -y, --no-prompt
              Don't prompt for input; accept all defaults when running examples.

          --prompt-inputs <values>
              Don't prompt for input; comma-delimited list of inputs for examples that
              accept user input.

          --example-dir <dir>
              Override the directory containing example configurations.

          --force
              Override warning when attempting to start Solr as the root user.

          --verbose
              Set log level to DEBUG (verbose); default is INFO

          -q, --quiet
              Set log level to WARN (quiet); default is INFO
        """);
  }
}

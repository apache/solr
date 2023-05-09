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

import static org.apache.solr.packagemanager.PackageUtils.print;
import static org.apache.solr.packagemanager.PackageUtils.printGreen;

import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.exec.OS;

public class PostTool extends ToolBase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public PostTool() {
        this(CLIO.getOutStream());
    }

    public PostTool(PrintStream stdout) {
        super(stdout);
    }

    private static final boolean isWindows = (OS.isFamilyDOS() || OS.isFamilyWin9x() || OS.isFamilyWindows());
    private static final String THIS_SCRIPT = (isWindows ? "bin\\solr.cmd post" : "bin/solr post");

    @Override
    public String getName() {
        return "post";
    }

    @Override
    public List<Option> getOptions() {
        return List.of(
                Option.builder("url")
                        .argName("url")
                        .hasArg()
                        .required(true)
                        .desc("<base Solr update URL>")
                        .build(),
                Option.builder("host")
                        .argName("host")
                        .hasArg()
                        .required(false)
                        .desc("<host> (default: localhost)")
                        .build(),
                Option.builder("p")
                        .argName("port")
                        .hasArg()
                        .required(false)
                        .desc("Number of shards; default is 1")
                        //.withLongOpt("port")
                        .build(),
                Option.builder("commit")
                        .argName("yes|no")
                        .hasArg(true)
                        .required(false)
                        .desc("Whether to commit at end of post (default: yes)")
                        .build(),
                Option.builder("h")
                        .argName("help")
                        .hasArg(false)
                        .required(false)
                        .desc("prints tool help")
                        //.withLongOpt("help")
                        .build(),
                Option.builder("recursive")
                        .argName("depth")
                        .hasArg(true)
                        .required(false)
                        .desc("default: 1")
                        .build(),
                Option.builder("delay")
                        .argName("seconds")
                        .hasArg(true)
                        .required(false)
                        .desc("default: 10 for web, 0 for files")
                        .build(),
                Option.builder("type")
                        .argName("content-type")
                        .hasArg(true)
                        .required(false)
                        .desc("default: application/xml")
                        .build(),
                Option.builder("filetypes")
                        .argName("<type>[,<type>,...]")
                        .hasArg(true)
                        .required(false)
                        .desc("default: xml,json,jsonl,csv,pdf,doc,docx,ppt,pptx,xls,xlsx,odt,odp,ods,ott,otp,ots,rtf,htm,html,txt,log")
                        .build(),
                Option.builder("params")
                        .argName("<key>=<value>[&<key>=<value>...]")
                        .hasArg(true)
                        .required(false)
                        .desc("values must be URL-encoded; these pass through to Solr update request")
                        .build(),
                Option.builder("out")
                        .argName("yes|no")
                        .hasArg(true)
                        .required(false)
                        .desc("default: no; yes outputs Solr response to console")
                        .build(),
                Option.builder("format")
                        .argName("solr")
                        .hasArg(true)
                        .required(false)
                        .desc("sends application/json content as Solr commands to /update instead of /update/json/docs")
                        .build());

    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);

        String solrUrl = cli.getOptionValue("url");

        Map<String,String> props = Collections.singletonMap("-Dauto", "yes");
        boolean recursive = false;
        String[] args = cli.getArgs();
        String mode = SimplePostTool.DEFAULT_DATA_MODE;
        boolean auto = true;
        String type = SimplePostTool.DEFAULT_CONTENT_TYPE;
        String format = ""; // i.e not solr formatted json commands
        int delay = 0;
        String fileTypes ="";
        OutputStream out;
        Boolean commit = false;
        Boolean optimize = false;

        // this is werid way to pass it in!
        System.setProperty("url",solrUrl);

        //SimplePostTool spt = new SimplePostTool(mode, solrUrl, auto, type, format, recursive, delay, fileTypes, out, commit, optimize, args);
        SimplePostTool spt = SimplePostTool.parseArgsAndInit(cli.getArgs());
        spt.execute();
    }

    private static void displayToolOptions() {
        // Could use HelpFormatter, but want more control over formatting...
        System.out.println(
                "\n" +
                        "Usage: post -c <collection> [OPTIONS] <files|directories|urls|-d [\"...\",...]\n" +
                        "    or post -hel\n" +
                        "\n" +
                        "   collection name defaults to DEFAULT_SOLR_COLLECTION if not specifie\n" +
                        "\n" +
                        "OPTION\n" +
                        "======\n" +
                        "  Solr options\n" +
                        "    -url <base Solr update URL> (overrides collection, host, and port\n" +
                        "    -host <host> (default: localhost\n" +
                        "    -p or -port <port> (default: 8983\n" +
                        "    -commit yes|no (default: yes\n" +
                        "\n" +
                        "  Web crawl options\n" +
                        "    -recursive <depth> (default: 1\n" +
                        "    -delay <seconds> (default: 10\n" +
                        "\n" +
                        "  Directory crawl options\n" +
                        "    -delay <seconds> (default: 0\n" +
                        "\n" +
                        "  stdin/args options\n" +
                        "    -type <content/type> (default: application/xml\n" +
                        "\n" +
                        "  Other options\n" +
                        "    -filetypes <type>[,<type>,...] (default: xml,json,jsonl,csv,pdf,doc,docx,ppt,pptx,xls,xlsx,odt,odp,ods,ott,otp,ots,rtf,htm,html,txt,log\n" +
                        "    -params \"<key>=<value>[&<key>=<value>...]\" (values must be URL-encoded; these pass through to Solr update request\n" +
                        "    -out yes|no (default: no; yes outputs Solr response to console\n" +
                        "    -format solr (sends application/json content as Solr commands to /update instead of /update/json/docs\n" +
                        "\n" +
                        "\n" +
                        "Examples\n" +
                        "\n" +
                        "* JSON file:\n" + THIS_SCRIPT + " -c wizbang events.json" +
                        "* XML files:\n" + THIS_SCRIPT + " -c records article*.xm\n" +
                        "* CSV file:\n" + THIS_SCRIPT + " -c signals LATEST-signals.cs\n" +
                        "* Directory of files:\n" + THIS_SCRIPT + " -c myfiles ~/Document\n" +
                        "* Web crawl:\n" + THIS_SCRIPT + " -c gettingstarted http://lucene.apache.org/solr -recursive 1 -delay \n" +
                        "* Standard input (stdin): echo '{commit: {}}' |\n" + THIS_SCRIPT + " -c my_collection -type application/json -out yes -\n" +
                        "* Data as string:\n" + THIS_SCRIPT + " -c signals -type text/csv -out yes -d $'id,value\\n1,0.47'"
        );
    }
    public void printHelp(){
        print("Simple Post Tool\n---------------");
        printGreen("./solr post -c <collection> [OPTIONS] <files|directories|urls|-d [\"...\",...]>'");
        print("    or post -help");
        print("");
        printGreen("./solr package add-key <file-containing-trusted-key>");
        print("Add a trusted key to Solr.");
        print("");
        printGreen("./solr package install <package-name>[:<version>] ");
        print(
                "Install a package into Solr. This copies over the artifacts from the repository into Solr's internal package store and sets up classloader for this package to be used.");
        print("");
        printGreen(
                "./solr package deploy <package-name>[:<version>] [-y] [--update] -collections <comma-separated-collections> [-p <param1>=<val1> -p <param2>=<val2> ...] ");
        print(
                "Bootstraps a previously installed package into the specified collections. It the package accepts parameters for its setup commands, they can be specified (as per package documentation).");
        print("");
        printGreen("./solr package list-installed");
        print("Print a list of packages installed in Solr.");
        print("");
        printGreen("./solr package list-available");
        print("Print a list of packages available in the repositories.");
        print("");
        printGreen("./solr package list-deployed -c <collection>");
        print("Print a list of packages deployed on a given collection.");
        print("");
        printGreen("./solr package list-deployed <package-name>");
        print("Print a list of collections on which a given package has been deployed.");
        print("");
        printGreen(
                "./solr package undeploy <package-name> -collections <comma-separated-collections>");
        print("Undeploys a package from specified collection(s)");
        print("");
        printGreen("./solr package uninstall <package-name>:<version>");
        print(
                "Uninstall an unused package with specified version from Solr. Both package name and version are required.");
        print("\n");
        print(
                "Note: (a) Please add '-solrUrl http://host:port' parameter if needed (usually on Windows).");
        print(
                "      (b) Please make sure that all Solr nodes are started with '-Denable.packages=true' parameter.");
        print("\n");
    }


}

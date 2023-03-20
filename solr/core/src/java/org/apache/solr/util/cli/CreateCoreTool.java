package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.common.params.CommonParams.NAME;

public class CreateCoreTool extends ToolBase {

    public CreateCoreTool() {
        this(CLIO.getOutStream());
    }

    public CreateCoreTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public String getName() {
        return "create_core";
    }

    @Override
    public Option[] getOptions() {
        return new Option[]{
                SolrCLI.OPTION_SOLRURL,
                Option.builder(NAME)
                        .argName("NAME")
                        .hasArg()
                        .required(true)
                        .desc("Name of the core to create.")
                        .build(),
                Option.builder("confdir")
                        .argName("CONFIG")
                        .hasArg()
                        .required(false)
                        .desc(
                                "Configuration directory to copy when creating the new core; default is "
                                        + SolrCLI.DEFAULT_CONFIG_SET
                                        + '.')
                        .build(),
                Option.builder("configsetsDir")
                        .argName("DIR")
                        .hasArg()
                        .required(true)
                        .desc("Path to configsets directory on the local system.")
                        .build(),
                SolrCLI.OPTION_VERBOSE
        };
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.DEFAULT_SOLR_URL);
        if (!solrUrl.endsWith("/")) solrUrl += "/";

        File configsetsDir = new File(cli.getOptionValue("configsetsDir"));
        if (!configsetsDir.isDirectory())
            throw new FileNotFoundException(configsetsDir.getAbsolutePath() + " not found!");

        String configSet = cli.getOptionValue("confdir", SolrCLI.DEFAULT_CONFIG_SET);
        File configSetDir = new File(configsetsDir, configSet);
        if (!configSetDir.isDirectory()) {
            // we allow them to pass a directory instead of a configset name
            File possibleConfigDir = new File(configSet);
            if (possibleConfigDir.isDirectory()) {
                configSetDir = possibleConfigDir;
            } else {
                throw new FileNotFoundException(
                        "Specified config directory "
                                + configSet
                                + " not found in "
                                + configsetsDir.getAbsolutePath());
            }
        }

        String coreName = cli.getOptionValue(NAME);

        String systemInfoUrl = solrUrl + "admin/info/system";
        CloseableHttpClient httpClient = SolrCLI.getHttpClient();
        String coreRootDirectory = null; // usually same as solr home, but not always
        try {
            Map<String, Object> systemInfo = SolrCLI.getJson(httpClient, systemInfoUrl, 2, true);
            if ("solrcloud".equals(systemInfo.get("mode"))) {
                throw new IllegalStateException(
                        "Solr at "
                                + solrUrl
                                + " is running in SolrCloud mode, please use create_collection command instead.");
            }

            // convert raw JSON into user-friendly output
            coreRootDirectory = (String) systemInfo.get("core_root");

            // Fall back to solr_home, in case we are running against older server that does not return
            // the property
            if (coreRootDirectory == null) coreRootDirectory = (String) systemInfo.get("solr_home");
            if (coreRootDirectory == null)
                coreRootDirectory = configsetsDir.getParentFile().getAbsolutePath();

        } finally {
            SolrCLI.closeHttpClient(httpClient);
        }

        String coreStatusUrl = solrUrl + "admin/cores?action=STATUS&core=" + coreName;
        if (SolrCLI.safeCheckCoreExists(coreStatusUrl, coreName)) {
            throw new IllegalArgumentException(
                    "\nCore '"
                            + coreName
                            + "' already exists!\nChecked core existence using Core API command:\n"
                            + coreStatusUrl);
        }

        File coreInstanceDir = new File(coreRootDirectory, coreName);
        File confDir = new File(configSetDir, "conf");
        if (!coreInstanceDir.isDirectory()) {
            coreInstanceDir.mkdirs();
            if (!coreInstanceDir.isDirectory())
                throw new IOException(
                        "Failed to create new core instance directory: " + coreInstanceDir.getAbsolutePath());

            if (confDir.isDirectory()) {
                FileUtils.copyDirectoryToDirectory(confDir, coreInstanceDir);
            } else {
                // hmmm ... the configset we're cloning doesn't have a conf sub-directory,
                // we'll just assume it is OK if it has solrconfig.xml
                if ((new File(configSetDir, "solrconfig.xml")).isFile()) {
                    FileUtils.copyDirectory(configSetDir, new File(coreInstanceDir, "conf"));
                } else {
                    throw new IllegalArgumentException(
                            "\n"
                                    + configSetDir.getAbsolutePath()
                                    + " doesn't contain a conf subdirectory or solrconfig.xml\n");
                }
            }
            echoIfVerbose(
                    "\nCopying configuration to new core instance directory:\n"
                            + coreInstanceDir.getAbsolutePath(),
                    cli);
        }

        String createCoreUrl =
                String.format(
                        Locale.ROOT,
                        "%sadmin/cores?action=CREATE&name=%s&instanceDir=%s",
                        solrUrl,
                        coreName,
                        coreName);

        echoIfVerbose(
                "\nCreating new core '" + coreName + "' using command:\n" + createCoreUrl + "\n", cli);

        try {
            Map<String, Object> json = SolrCLI.getJson(createCoreUrl);
            if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
                CharArr arr = new CharArr();
                new JSONWriter(arr, 2).write(json);
                echo(arr.toString());
                echo("\n");
            } else {
                echo(String.format(Locale.ROOT, "\nCreated new core '%s'", coreName));
            }
        } catch (Exception e) {
            /* create-core failed, cleanup the copied configset before propagating the error. */
            PathUtils.deleteDirectory(coreInstanceDir.toPath());
            throw e;
        }
    }
} // end CreateCoreTool class

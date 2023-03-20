package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;

import java.io.PrintStream;

public abstract class ToolBase implements Tool {
    protected PrintStream stdout;
    protected boolean verbose = false;

    protected ToolBase() {
        this(CLIO.getOutStream());
    }

    protected ToolBase(PrintStream stdout) {
        this.stdout = stdout;
    }

    protected void echoIfVerbose(final String msg, CommandLine cli) {
        if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
            echo(msg);
        }
    }

    protected void echo(final String msg) {
        stdout.println(msg);
    }

    @Override
    public int runTool(CommandLine cli) throws Exception {
        verbose = cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt());

        int toolExitStatus = 0;
        try {
            runImpl(cli);
        } catch (Exception exc) {
            // since this is a CLI, spare the user the stacktrace
            String excMsg = exc.getMessage();
            if (excMsg != null) {
                CLIO.err("\nERROR: " + excMsg + "\n");
                if (verbose) {
                    exc.printStackTrace(CLIO.getErrStream());
                }
                toolExitStatus = 1;
            } else {
                throw exc;
            }
        }
        return toolExitStatus;
    }

    public abstract void runImpl(CommandLine cli) throws Exception;
}

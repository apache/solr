package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/** Defines the interface to a Solr tool that can be run from the command-line app. */
public interface Tool {
    String getName();

    Option[] getOptions();

    int runTool(CommandLine cli) throws Exception;
}

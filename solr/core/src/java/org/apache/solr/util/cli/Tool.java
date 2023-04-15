package org.apache.solr.util.cli;

import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public interface Tool {
  /** Defines the interface to a Solr tool that can be run from this command-line app. */
  String getName();

  List<Option> getOptions();

  int runTool(CommandLine cli) throws Exception;
}

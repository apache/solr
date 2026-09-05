package org.apache.solr.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.packagemanager.PackageManager;
import org.apache.solr.packagemanager.RepositoryManager;

import static org.apache.solr.packagemanager.PackageUtils.format;
import static org.apache.solr.packagemanager.PackageUtils.formatGreen;

/** Supports package add-repo command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "add-repo",
    description = "Command to add repository for package management in SolrCloud.",
    footerHeading = "%nExamples:%n",
    footer = {
        "  # Add a package repository",
        "  bin/solr package add-repo myrepo https://my.repo.example/repo",
    })
public class AddRepo extends ToolBase {

  public PackageManager packageManager;
  public RepositoryManager repositoryManager;

  @Override
  public void runImpl(CommandLine cli) throws Exception {

  }

  @Override
  public int callTool() throws Exception {
    return 0;
  }

  @Override
  public String getName() {
    return "add-repo";
  }

  @Override
  public String getHeader() {
    StringBuilder sb = new StringBuilder();
    format(sb, "Package Manager\n---------------");
    formatGreen(sb, "bin/solr package add-repo <repository-name> <repository-url>");
    format(sb, "Add a repository to Solr.");
    format(sb, "");
    return sb.toString();
  }
}

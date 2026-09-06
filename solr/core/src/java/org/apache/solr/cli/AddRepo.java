package org.apache.solr.cli;

import static org.apache.solr.cli.SolrCLI.printGreen;
import static org.apache.solr.packagemanager.PackageUtils.format;
import static org.apache.solr.packagemanager.PackageUtils.formatGreen;

import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.packagemanager.PackageManager;
import org.apache.solr.packagemanager.RepositoryManager;

/** Supports package add-repo command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "add-repo",
    description = "Add a package repository to Solr.",
    footerHeading = "%nExamples:%n",
    footer = {
        "  # Add a package repository",
        "  bin/solr package add-repo myrepo https://my.repo.example/repo",
    })
public class AddRepo extends ToolBase {

  @picocli.CommandLine.ParentCommand private PackageTool packageTool;

  @picocli.CommandLine.Mixin private HelpMixin helpMixin;
  @picocli.CommandLine.Mixin private CredentialsOptions credentialsOptions;

  @picocli.CommandLine.ArgGroup private ConnectionOptions connectionOptions;

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      paramLabel = "REPOSITORY-NAME")
  private String repoName;

  @picocli.CommandLine.Parameters(
      index = "1",
      arity = "1",
      paramLabel = "REPOSITORY-URL")
  private String repoUrl;

  public AddRepo() {
    this(new DefaultToolRuntime());
  }

  public AddRepo(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    throw new UnsupportedOperationException("add-repo is implemented via the commons-cli path under PackageTool");
  }

  @Override
  public int callTool() throws Exception {
    String credentials = credentialsOptions.credentials;
    String solrUrl = packageTool.resolveSolrUrl(credentials);
    String zkHost = packageTool.resolveZkHost(solrUrl, credentials);
    addRepo(solrUrl, zkHost, credentials, repoName, repoUrl);
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

  private void addRepo(String solrUrl, String zkHost, String credentials, String repoName, String repoUrl) throws Exception {
    Level oldLevel = LoggerContext.getContext(false).getRootLogger().getLevel();
    Configurator.setRootLevel(Level.OFF);

    try {
      if (zkHost == null) {
        throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "Package manager only runs in SolrCloud");
      }


      try (SolrClient solrClient = CLIUtils.getSolrClient(solrUrl, credentials, true)) {
        PackageManager packageManager = new PackageManager(packageTool.getRuntime(), solrClient, solrUrl, zkHost);
        try {
        RepositoryManager repositoryManager = new RepositoryManager(solrClient, packageManager);
        repositoryManager.addRepository(repoName, repoUrl);
        printGreen("Added repository: " + repoName);
        } finally {
        packageManager.close();
        }
      }
    } catch (Exception exception) {
      exception.printStackTrace(System.err);
      throw exception;
    } finally {
      Configurator.setRootLevel(oldLevel);
    }

  }
}

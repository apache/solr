package org.apache.solr.handler.component;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.stream.EchoStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
public class LogStreamTest extends SolrCloudTestCase {
  private static StreamFactory factory;
  private static StreamContext context;
  private static final String COLLECTION = "streams";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "config", 2, 1, 1, 0)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2 * (1 + 1));

    String zkHost = cluster.getZkServer().getZkAddress();
    factory =
        new StreamFactory()
            .withCollectionZkHost(COLLECTION, zkHost)
            .withFunctionName("logging", LogStream.class)
            .withFunctionName("echo", EchoStream.class);

    final Path dataDir = findUserFilesDataDir();
    Files.createDirectories(dataDir);

    context = new StreamContext();
    context.put("solr-core", findSolrCore());
    SolrClientCache solrClientCache = new SolrClientCache();

    context.setSolrClientCache(solrClientCache);
  }

  @Test
  public void testLogStreamExpressionToExpression() throws Exception {
    String expressionString;

    // Basic test
    try (LogStream stream =
        new LogStream(StreamExpressionParser.parse("logging(bob.txt,echo(\"bob\"))"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("logging(bob.txt,"));
      assertTrue(expressionString.contains("echo(\"bob"));
    }

    // Unwrap double quotes around file name test
    try (LogStream stream =
        new LogStream(
            StreamExpressionParser.parse("logging(\"outputs/bob.txt\",echo(\"bob\"))"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("logging(outputs/bob.txt,"));
      assertTrue(expressionString.contains("echo(\"bob"));
    }
  }

  @Test
  public void testFileOutputDirectoryPermissions() throws Exception {

    LogStream stream =
        new LogStream(StreamExpressionParser.parse("logging(/tmp/bob.txt,echo(\"bob\"))"), factory);
    stream.setStreamContext(context);

    LogStream finalStream1 = stream;
    SolrException thrown =
        assertThrows(
            "Attempting to write to /tmp should be prevented",
            SolrException.class,
            () -> finalStream1.open());
    assertTrue(thrown.getMessage().startsWith("file to log to must be under "));

    stream =
        new LogStream(StreamExpressionParser.parse("logging(../bob.txt,echo(\"bob\"))"), factory);
    stream.setStreamContext(context);

    LogStream finalStream2 = stream;
    thrown =
        assertThrows(
            "Attempting to escape the userfiles directory should be prevented",
            SolrException.class,
            () -> finalStream2.open());
    assertTrue(thrown.getMessage().startsWith("file to log to must be under "));
  }

  private static Path findUserFilesDataDir() {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      for (CoreDescriptor coreDescriptor : jetty.getCoreContainer().getCoreDescriptors()) {
        if (coreDescriptor.getCollectionName().equals(COLLECTION)) {
          return jetty.getCoreContainer().getUserFilesPath();
        }
      }
    }

    throw new IllegalStateException("Unable to determine data-dir for: " + COLLECTION);
  }

  private static SolrCore findSolrCore() {
    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      for (SolrCore solrCore : solrRunner.getCoreContainer().getCores()) {
        if (solrCore != null) {
          return solrCore;
        }
      }
    }
    throw new RuntimeException("Didn't find any valid cores.");
  }
}

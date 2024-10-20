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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CsvStream;
import org.apache.solr.client.solrj.io.stream.EchoStream;
import org.apache.solr.client.solrj.io.stream.ListStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
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
public class LoggingStreamTest extends SolrCloudTestCase {
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
            .withFunctionName("logging", LoggingStream.class)
            .withFunctionName("echo", EchoStream.class)
            .withFunctionName("parseCSV", CsvStream.class)
            .withFunctionName("list", ListStream.class)
            .withFunctionName("tuple", TupStream.class);

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
    try (LoggingStream stream =
        new LoggingStream(
            StreamExpressionParser.parse("logging(bob.txt,echo(\"bob\"))"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("logging(bob.txt,"));
      assertTrue(expressionString.contains("echo(\"bob"));
    }

    // Unwrap double quotes around file name test
    try (LoggingStream stream =
        new LoggingStream(
            StreamExpressionParser.parse("logging(\"outputs/bob.txt\",echo(\"bob\"))"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("logging(outputs/bob.txt,"));
      assertTrue(expressionString.contains("echo(\"bob"));
    }
  }

  @Test
  public void testLogStreamExpressionToExplanation() throws Exception {

    try (LoggingStream stream =
        new LoggingStream(
            StreamExpressionParser.parse("logging(bob.txt,echo(\"bob\"))"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      assertEquals("logging (bob.txt)", explanation.getFunctionName());
      assertEquals(LoggingStream.class.getName(), explanation.getImplementingClass());
    }
  }

  @Test
  public void testFileOutputDirectoryPermissions() throws Exception {

    LoggingStream stream =
        new LoggingStream(
            StreamExpressionParser.parse("logging(/tmp/bob.txt,echo(\"bob\"))"), factory);
    stream.setStreamContext(context);

    LoggingStream finalStream1 = stream;
    SolrException thrown =
        assertThrows(
            "Attempting to write to /tmp should be prevented",
            SolrException.class,
            () -> finalStream1.open());
    assertTrue(thrown.getMessage().startsWith("file to log to must be under "));

    stream =
        new LoggingStream(
            StreamExpressionParser.parse("logging(../bob.txt,echo(\"bob\"))"), factory);
    stream.setStreamContext(context);

    LoggingStream finalStream2 = stream;
    thrown =
        assertThrows(
            "Attempting to escape the userfiles directory should be prevented",
            SolrException.class,
            () -> finalStream2.open());
    assertTrue(thrown.getMessage().startsWith("file to log to must be under "));
  }

  @Test
  public void testLoggingStreamCombinedWithCatAndJsonStream() throws Exception {
    String expr =
        "logging(parsed_csv_output.jsonl,"
            + "parseCSV(list(tuple(file=\"file1\", line=\"a,b,c\"), "
            + "                        tuple(file=\"file1\", line=\"1,2,3\"),"
            + "                        tuple(file=\"file1\", line=\"\\\"hello, world\\\",9000,20\"),"
            + "                        tuple(file=\"file2\", line=\"field_1,field_2,field_3\"), "
            + "                        tuple(file=\"file2\", line=\"8,9,\")))"
            + ")";

    try (LoggingStream stream = new LoggingStream(StreamExpressionParser.parse(expr), factory)) {
      stream.setStreamContext(context);
      List<Tuple> tuples = getTuples(stream);
      assertEquals(tuples.size(), 3);
      assertEquals(tuples.get(0).getString("totalIndexed"), "1");
      assertEquals(tuples.get(0).getString("batchLogged"), "1");
      assertEquals(tuples.get(0).getString("batchNumber"), "1");

      assertEquals(tuples.get(1).getString("totalIndexed"), "2");
      assertEquals(tuples.get(1).getString("batchLogged"), "1");
      assertEquals(tuples.get(1).getString("batchNumber"), "2");

      assertEquals(tuples.get(2).getString("totalIndexed"), "3");
      assertEquals(tuples.get(2).getString("batchLogged"), "1");
      assertEquals(tuples.get(2).getString("batchNumber"), "3");
    }
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

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<>();

    try (tupleStream) {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
      }
    }
    return tuples;
  }
}

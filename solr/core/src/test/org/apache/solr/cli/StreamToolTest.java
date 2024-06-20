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

import static org.apache.solr.cli.SolrCLI.findTool;
import static org.apache.solr.cli.SolrCLI.parseCmdLine;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.SecurityJson;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamToolTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupClusterWithSecurityEnabled() throws Exception {
    configureCluster(2).withSecurityJson(SecurityJson.SIMPLE).configure();
  }

  private <T extends SolrRequest<? extends SolrResponse>> T withBasicAuth(T req) {
    req.setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS);
    return req;
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testGetHeaderFromFirstTuple() {
    Tuple tuple = new Tuple(new HashMap());
    tuple.put("field1", "blah");
    tuple.put("field2", "blah");
    tuple.put("field3", "blah");

    String[] headers = StreamTool.getHeadersFromFirstTuple(tuple);

    assertEquals(headers.length, 3);
    assertEquals(headers[0], "field1");
    assertEquals(headers[1], "field2");
    assertEquals(headers[2], "field3");
  }

  @Test
  public void testGetOutputFields() {
    String[] args =
        new String[] {
          "--fields", "field9, field2, field3, field4",
        };
    StreamTool streamTool = new StreamTool();
    CommandLine cli =
        SolrCLI.processCommandLineArgs(streamTool.getName(), streamTool.getOptions(), args);
    String[] outputFields = StreamTool.getOutputFields(cli);
    assert outputFields != null;
    assertEquals(outputFields.length, 4);
    assertEquals(outputFields[0], "field9");
    assertEquals(outputFields[1], "field2");
    assertEquals(outputFields[2], "field3");
    assertEquals(outputFields[3], "field4");
  }

  @Test
  public void testReadExpression() throws Exception {
    // This covers parameter substitution and expanded comments support.

    String[] args = {"file.expr", "one", "two", "three"};
    StringWriter stringWriter = new StringWriter();
    PrintWriter buf = new PrintWriter(stringWriter);
    buf.println("/*");
    buf.println("Multi-line comment Comment...");
    buf.println("*/");
    buf.println("// Single line comment");
    buf.println("# Single line comment");
    buf.println("let(a=$1, b=$2,");
    buf.println("search($3))");
    buf.println(")");

    String expr = stringWriter.toString();

    LineNumberReader reader = new LineNumberReader(new StringReader(expr));
    String finalExpression = StreamTool.readExpression(reader, args);
    // Strip the comment and insert the params in order.
    assertEquals(finalExpression, "let(a=one, b=two,search(three)))");
  }

  @Test
  public void testReadExpression2() throws Exception {
    // This covers parameter substitution and expanded comments support.

    String[] args = {"file.expr", "id", "desc_s", "desc"};
    StringWriter stringWriter = new StringWriter();
    PrintWriter buf = new PrintWriter(stringWriter);

    buf.println("# Try me");
    buf.println("search(my_collection,q='*:*',fl='$1, $2',sort='id $3')");

    String expr = stringWriter.toString();

    LineNumberReader reader = new LineNumberReader(new StringReader(expr));
    String finalExpression = StreamTool.readExpression(reader, args);
    // Strip the comment and insert the params in order.
    assertEquals(finalExpression, "search(my_collection,q='*:*',fl='id, desc_s',sort='id desc')");
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testReadStream() throws Exception {
    StreamTool.StandardInStream inStream = new StreamTool.StandardInStream();
    List<Tuple> tuples = new ArrayList();
    try {
      StringWriter stringWriter = new StringWriter();
      PrintWriter buf = new PrintWriter(stringWriter);

      buf.println("one  two");
      buf.println("three  four");
      buf.println("five  six");

      String expr = stringWriter.toString();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(expr.getBytes());
      inStream.setInputStream(inputStream);
      inStream.open();
      while (true) {
        Tuple tuple = inStream.read();
        if (tuple.EOF) {
          break;
        } else {
          tuples.add(tuple);
        }
      }

    } finally {
      inStream.close();
    }

    assertEquals(tuples.size(), 3);

    String line1 = tuples.get(0).getString("line");
    String line2 = tuples.get(1).getString("line");
    String line3 = tuples.get(2).getString("line");

    assertEquals("one  two", line1);
    assertEquals("three  four", line2);
    assertEquals("five  six", line3);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testListToString() {
    List stuff = new ArrayList();
    stuff.add("test1");
    stuff.add(3);
    stuff.add(111.32322);
    stuff.add("test3");
    String s = StreamTool.listToString(stuff, "|");
    assertEquals("test1|3|111.32322|test3", s);
  }

  @Test
  public void testRunEchoStreamLocally() throws Exception {

    String expression = "echo(Hello)";
    File expressionFile = File.createTempFile("expression", ".EXPR");
    FileWriter writer = new FileWriter(expressionFile);
    writer.write(expression);
    writer.close();

    // test passing in the file
    // notice that we do not pass in zkHost or solrUrl for a simple echo run locally.
    String[] args = {"stream", "-workers", "local", "-verbose", expressionFile.getAbsolutePath()};

    assertEquals(0, runTool(args));

    // test passing in the expression directly
    args = new String[] {"stream", "-workers", "local", "-verbose", expression};

    assertEquals(0, runTool(args));
  }

  @Test
  public void testRunEchoStreamRemotely() throws Exception {
    String collectionName = "streamWorkerCollection";
    withBasicAuth(CollectionAdminRequest.createCollection(collectionName, "_default", 1, 1))
        .processAndWait(cluster.getSolrClient(), 10);
    waitForState(
        "Expected collection to be created with 1 shard and 1 replicas",
        collectionName,
        clusterShape(1, 1));

    String expression = "echo(Hello)";
    File expressionFile = File.createTempFile("expression", ".EXPR");
    FileWriter writer = new FileWriter(expressionFile);
    writer.write(expression);
    writer.close();

    // test passing in the file
    String[] args = {
      "stream",
      "-workers",
      "remote",
      "-name",
      collectionName,
      "-verbose",
      "-zkHost",
      cluster.getZkClient().getZkServerAddress(),
      "-credentials",
      SecurityJson.USER_PASS,
      expressionFile.getAbsolutePath()
    };

    assertEquals(0, runTool(args));

    // test passing in the expression directly
    args =
        new String[] {
          "stream",
          "-workers",
          "remote",
          "-name",
          collectionName,
          "-verbose",
          "-zkHost",
          cluster.getZkClient().getZkServerAddress(),
          "-credentials",
          SecurityJson.USER_PASS,
          expression
        };

    assertEquals(0, runTool(args));
  }

  private int runTool(String[] args) throws Exception {
    Tool tool = findTool(args);
    assertTrue(tool instanceof StreamTool);
    CommandLine cli = parseCmdLine(tool.getName(), args, tool.getOptions());
    return tool.runTool(cli);
  }
}

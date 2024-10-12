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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.solr.client.solrj.io.Lang;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class UBIComponentLocalLoggingTest extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testLocalCatStream() throws Exception {

    CollectionAdminRequest.createCollection(COLLECTION, "config", 2, 1, 1, 0)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2 * (1 + 1));

    File localFile = File.createTempFile("topLevel1", ".txt");

    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    // Replica rr = zkStateReader.getCollection(coll).getReplicas().get(0);

    // cluster.getJettySolrRunner(0).getCoreContainer().getCore()
    //        Replica replica =
    //              getRandomReplica(
    //                    shard, (r) -> (r.getState() == Replica.State.ACTIVE &&
    // !r.equals(shard.getLeader())));

    SolrCore solrCoreToLoad = null;
    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      for (SolrCore solrCore : solrRunner.getCoreContainer().getCores()) {
        if (solrCore != null) {
          solrCoreToLoad = solrCore;
        }
        System.out.println(solrCore);
      }
    }

    final Path dataDir = findUserFilesDataDir();
    Files.createDirectories(dataDir);
    // populateFileStreamData(dataDir);

    // JettySolrRunner replicaJetty = cluster.getReplicaJetty(replica);
    // cluster.getJettySolrRunner(0).getCoreContainer().getr

    // SolrQueryRequest req = req("q", "*:*");
    CoreContainer cc = cluster.getJettySolrRunner(0).getCoreContainer();

    var l = cc.getAllCoreNames();
    SolrCore core = cc.getCore(l.get(0));
    streamContext.put("solr-core", core);
    SolrClientCache solrClientCache = new SolrClientCache();

    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory streamFactory = new StreamFactory();

    // LocalCatStream extends CatStream and disables the Solr cluster specific
    // logic about where to read data from.
    streamFactory.withFunctionName("logging", LoggingStream.class);

    Lang.register(streamFactory);

    String clause = "logging(bob.txt,echo(\"bob\"))";
    stream = streamFactory.constructStream(clause);
    stream.setStreamContext(streamContext);
    tuples = getTuples(stream);
    stream.close();
    solrClientCache.close();

    // populateFileWithData(localFile.toPath());

    Tuple tuple = new Tuple(new HashMap());
    tuple.put("field1", "blah");
    tuple.put("field2", "blah");
    tuple.put("field3", "blah");

    // LoggingStream logStream =
    ///    //   new LoggingStream(localFile.getAbsolutePath());
    // LoggingStream logStream =
    //              new LoggingStream();
    List<Tuple> tuples2 = new ArrayList();
    try {
      // logStream.open();

      //            while (true) {
      //                Tuple tuple = logStream.read();
      //                if (tuple.EOF) {
      //                    break;
      //                } else {
      //                    tuples.add(tuple);
      //                }
      //            }

    } finally {
      //   logStream.close();
    }

    assertEquals(1, tuples.size());

    //    for (int i = 0; i < 1; i++) {
    //      Tuple t = tuples.get(i);
    //      assertEquals(localFile.getName() + " line " + (i + 1), t.get("line"));
    //      assertEquals(localFile.getAbsolutePath(), t.get("file"));
    //    }
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

  private List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for (; ; ) {
      Tuple t = tupleStream.read();
      // log.info(" ... {}", t.fields);
      if (t.EOF) {
        break;
      } else {
        tuples.add(t);
      }
    }
    tupleStream.close();
    return tuples;
  }
}

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
package org.apache.solr.bench.search;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.longs;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.solr.bench.Docs;
import org.apache.solr.client.solrj.request.JavaBinRequestWriter;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.XMLRequestWriter;
import org.apache.solr.common.SolrInputDocument;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark for serialization of requests by the client. This only focuses on the serialization
 * itself, ignoring sending the request over the network (and for sure ignoring processing the
 * request).
 */
@Fork(value = 1)
@BenchmarkMode(Mode.Throughput)
@Warmup(time = 2, iterations = 1)
@Measurement(time = 5, iterations = 5)
@Threads(value = 1)
public class RequestWriters {

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"xml", "binary"})
    String type;

    @Param({"10", "100", "1000", "10000"})
    int batchSize;

    private final int docCount = 50000;

    private Supplier<RequestWriter> writerSupplier;
    private Iterator<SolrInputDocument> docIterator;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      preGenerateDocs();

      switch (type) {
        case "xml":
          writerSupplier = XMLRequestWriter::new;
          break;
        case "javabin":
          writerSupplier = JavaBinRequestWriter::new;
          break;

        default:
          throw new Error("Unsupported type: " + type);
      }
    }

    private void preGenerateDocs() throws Exception {
      Docs docs =
          docs()
              .field("id", integers().incrementing())
              .field(strings().basicLatinAlphabet().ofLengthBetween(10, 64))
              .field(strings().basicLatinAlphabet().ofLengthBetween(10, 64))
              .field(strings().basicLatinAlphabet().multi(312).ofLengthBetween(10, 64))
              .field(strings().basicLatinAlphabet().multi(312).ofLengthBetween(10, 64))
              .field(integers().all())
              .field(integers().all())
              .field(longs().all());

      docs.preGenerate(docCount);
      docIterator = docs.generatedDocsCircularIterator();
    }
  }

  @Benchmark
  public void writeUpdate(BenchState state) throws IOException {

    OutputStream sink = NullOutputStream.INSTANCE;

    UpdateRequest request = new UpdateRequest();
    for (int i = 0; i < state.batchSize; i++) {
      request.add(state.docIterator.next());
    }

    RequestWriter writer = state.writerSupplier.get();
    writer.write(request, sink);
  }
}

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
package org.apache.solr.bench.javabin;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.dates;
import static org.apache.solr.bench.generators.SourceDSL.doubles;
import static org.apache.solr.bench.generators.SourceDSL.floats;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.longs;
import static org.apache.solr.bench.generators.SourceDSL.maps;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.SolrGenerate;
import org.apache.solr.bench.SplittableRandomGenerator;
import org.apache.solr.bench.generators.LazyGen;
import org.apache.solr.bench.generators.NamedListGen;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.BytesOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;
import org.eclipse.jetty.io.RuntimeIOException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.quicktheories.api.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.BenchmarkRandomSource;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Warmup(time = 15, iterations = 5)
@Measurement(time = 20, iterations = 5)
@Fork(value = 1)
@Timeout(time = 60)
public class JavaBinBasicPerf {

  public static final int COUNT = 10;

  @State(Scope.Thread)
  public static class ThreadState {
    private final BytesOutputStream baos = new BytesOutputStream(1024 * 1024 * 24);
  }

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"1.0"})
    public float scale;

    @Param({
      "default"
    }) // nested, numeric, large_strings, very_large_text_and_strings, many_token_field,
    // small_strings
    public String content;

    private final Queue<byte[]> responseByteArrays = new ConcurrentLinkedQueue<>();
    private final Queue<Object> responses = new ConcurrentLinkedQueue<>();

    private volatile Iterator<Object> responseiterator;
    private volatile Iterator<byte[]> responseByteArrayIterator;

    @SuppressForbidden(reason = "NoMDCAwareNecessary")
    @Setup(Level.Trial)
    public void doSetup(BaseBenchState baseBenchState) throws Exception {

      BaseBenchState.log("scale=" + scale);
      ExecutorService executorService =
          Executors.newFixedThreadPool(
              Runtime.getRuntime().availableProcessors(),
              new SolrNamedThreadFactory("JavaBinPerf DataGen"));

      responseByteArrays.clear();
      responses.clear();

      AtomicBoolean stop = new AtomicBoolean(false);
      AtomicReference<Exception> failed = new AtomicReference<>();
      for (int i = 0; i < 100 && !stop.get(); i++) {
        int finalI = i;
        executorService.submit(
            () -> {
              try {
                Object response;
                switch (content) {
                  case "default":
                    if (scale > 2 && finalI >= 50) {
                      stop.set(true);
                    }

                    response = defaultContent(COUNT, scale);
                    break;
                  case "numeric":
                    response = numericsContent((int) (COUNT * scale));
                    break;
                  case "large_strings":
                    if (scale > 2 && finalI >= 10) {
                      stop.set(true);
                    }
                    response = largeStringsContent(COUNT, scale);
                    break;
                  case "very_large_text_and_strings":
                    if (finalI >= 10) {
                      stop.set(true);
                    }
                    response = veryLargeTextAndStrings(COUNT, scale);
                    break;
                  case "many_token_field":
                    response = manyTokenFieldContent(COUNT, scale);
                    break;
                  case "small_strings":
                    response = smallStrings(COUNT, scale);
                    break;
                  case "nested":
                    response = nested(baseBenchState, scale);
                    break;
                  default:
                    BaseBenchState.log(
                        String.format(Locale.ENGLISH, "Unknown content type: %s", content));
                    throw new IllegalArgumentException("Unknown content type: " + content);
                }

                try (final JavaBinCodec jbc = new JavaBinCodec()) {
                  BytesOutputStream baos = new BytesOutputStream(1024 << 8);
                  jbc.marshal(response, baos, true);
                  responseByteArrays.add(baos.toBytes());
                  responses.add(response);
                } catch (IOException e) {
                  BaseBenchState.log("IOException " + e.getMessage());
                  throw new RuntimeIOException(e);
                }
              } catch (Exception e) {
                e.printStackTrace();
                failed.set(e);
                executorService.shutdownNow();
              }
            });
      }

      if (failed.get() != null) {
        throw failed.get();
      }

      executorService.shutdown();
      boolean result = false;
      while (!result) {
        result = executorService.awaitTermination(600, TimeUnit.MINUTES);
      }

      BaseBenchState.log(
          "setup responses="
              + responses.size()
              + " responseByteArrays="
              + responseByteArrays.size());

      responseiterator = responses.iterator();
      responseByteArrayIterator = responseByteArrays.iterator();
    }

    public Object getResponse() {
      if (!responseiterator.hasNext()) {
        responseiterator = responses.iterator();
      }
      while (true) {
        try {
          return responseiterator.next();
        } catch (NoSuchElementException e) {
          responseiterator = responses.iterator();
        }
      }
    }

    public byte[] getResponseByteArray() {
      Iterator<byte[]> rbai = responseByteArrayIterator;
      if (!rbai.hasNext()) {
        rbai = responseByteArrays.iterator();
        responseByteArrayIterator = rbai;
      }
      while (true) {
        try {
          byte[] array = rbai.next();
          if (array == null) {
            throw new NoSuchElementException();
          }
          return array;
        } catch (NoSuchElementException e) {
          rbai = responseByteArrays.iterator();
          responseByteArrayIterator = rbai;
        }
      }
    }

    private Object nested(BaseBenchState baseBenchState, float scale) {
      SplittableRandomGenerator random =
          new SplittableRandomGenerator(BaseBenchState.getRandomSeed());

      Gen<? extends Map<String, ?>> mapGen =
          maps().of(getKey(), getValue(10)).ofSizeBetween((int) (20 * scale), (int) (30 * scale));

      // BaseBenchState.log("map:" + map);

      return mapGen.generate(new BenchmarkRandomSource(random));
    }

    private static SolrGen<String> getKey() {
      return strings().betweenCodePoints('a', 'z' + 1).ofLengthBetween(1, 10);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SolrGen<?> getValue(int depth) {
      if (depth == 0) {
        return integers().from(1).upToAndIncluding(5000);
      }
      List values = new ArrayList(4);
      values.add(
          Pair.of(
              4, maps().of(getKey(), new LazyGen(() -> getValue(depth - 1))).ofSizeBetween(1, 25)));
      values.add(
          Pair.of(
              4,
              new NamedListGen(
                  maps()
                      .of(getKey(), new LazyGen(() -> getValue(depth - 1)))
                      .ofSizeBetween(1, 35))));
      values.add(Pair.of(COUNT, integers().all()));
      values.add(Pair.of(16, longs().all()));
      values.add(Pair.of(8, doubles().all()));
      values.add(Pair.of(5, floats().all()));
      values.add(Pair.of(9, dates().all()));
      return SolrGenerate.frequency(values);
    }

    private static Object numericsContent(int count) {
      List<Object> topLevel = new ArrayList<>(16);
      for (int i = 0; i < count; i++) {
        List<Object> types = new ArrayList<>(16);

        types.add((short) 2);
        types.add((double) 3);

        types.add(-4);
        types.add(4);
        types.add(42);

        types.add((long) -56547532);
        types.add((long) 578675675);
        types.add((long) 500000);
        topLevel.add(types);
      }

      return topLevel;
    }

    private static Object defaultContent(int count, float scale) {
      NamedList<Object> response = new NamedList<>();

      NamedList<Object> header = new NamedList<>();
      header.add("status", 0);
      header.add("headerStuff", "values");
      response.add("header", header);

      Docs docs =
          docs()
              .field("id", integers().incrementing())
              .field(
                  "facet_s",
                  strings()
                      .basicLatinAlphabet()
                      .maxCardinality(5)
                      .ofLengthBetween(50, (int) (64 * scale)))
              .field(
                  "facet2_s",
                  strings().basicLatinAlphabet().maxCardinality(100).ofLengthBetween(12, 16))
              .field(
                  "facet3_s",
                  strings().basicLatinAlphabet().maxCardinality(1200).ofLengthBetween(110, 128))
              .field(
                  "text",
                  strings()
                      .basicLatinAlphabet()
                      .multi((int) (50 * scale))
                      .ofLengthBetween(10, (int) (100 * scale)))
              .field(
                  "text2_s",
                  strings()
                      .basicLatinAlphabet()
                      .multi((int) (150 * scale))
                      .ofLengthBetween(6, (int) (25 * scale)))
              .field(
                  "text3_t",
                  strings()
                      .basicLatinAlphabet()
                      .multi((int) (1000 * scale))
                      .ofLengthBetween(4, (int) (COUNT * scale)))
              .field("int_i", integers().all())
              .field("long1_l", longs().all())
              .field("long2_l", longs().all())
              .field("long3_l", longs().all())
              .field("int2_i", integers().allWithMaxCardinality(500));

      SolrDocumentList docList = new SolrDocumentList();
      for (int i = 0; i < count; i++) {
        SolrDocument doc = docs.document();
        docList.add(doc);
      }
      docList.setNumFound((long) scale);
      docList.setMaxScore(1.0f);
      docList.setStart(0);

      response.add("docs", docList);

      response.add("int", 42);
      response.add("long", 5000_023L);
      response.add("date", new Date());

      return response;
    }
  }

  @Benchmark
  @Timeout(time = 300)
  public Object encode(BenchState state, ThreadState threadState) throws Exception {
    try (final JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(state.getResponse(), threadState.baos, true);
      return threadState.baos;
    } finally {
      threadState.baos.reset();
    }
  }

  @Benchmark
  @Timeout(time = 300)
  public Object decode(BenchState state) throws Exception {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      return jbc.unmarshal(new ByteArrayInputStream(state.getResponseByteArray()));
    }
  }

  private static Object largeStringsContent(int count, float scale) {
    Docs docs =
        docs()
            .field(
                "string_s",
                strings().basicLatinAlphabet().ofLengthBetween(2000, (int) (2800 * scale)));

    SolrDocumentList docList = new SolrDocumentList();
    for (int i = 0; i < count * scale; i++) {
      SolrDocument doc = docs.document();
      docList.add(doc);
    }
    docList.setNumFound((long) (count * scale));
    docList.setMaxScore(1.0f);
    docList.setStart(0);

    return docList;
  }

  private static Object manyTokenFieldContent(int count, float scale) {
    Docs docs =
        docs()
            .field(
                "string_s",
                strings()
                    .basicLatinAlphabet()
                    .multi(Math.round(1500 * scale))
                    .ofLengthBetween(50, 100));
    SolrDocumentList docList = new SolrDocumentList();
    for (int i = 0; i < count; i++) {
      SolrDocument doc = docs.document();
      docList.add(doc);
    }
    docList.setNumFound(count);
    docList.setMaxScore(1.0f);
    docList.setStart(0);

    return docList;
  }

  private static Object smallStrings(int count, float scale) {
    NamedList<Object> response = new NamedList<>();

    NamedList<Object> header = new NamedList<>();
    header.add("status", 0);
    header.add("headerStuff", "values");
    response.add("header", header);

    Docs docs =
        docs()
            .field("id", integers().incrementing())
            .field(
                "facet_s",
                strings()
                    .basicLatinAlphabet()
                    .maxCardinality(5)
                    .ofLengthBetween(10, (int) (25 * scale)))
            .field(
                "facet2_s",
                strings().basicLatinAlphabet().maxCardinality(100).ofLengthBetween(6, 12))
            .field(
                "facet3_s",
                strings().basicLatinAlphabet().maxCardinality(1200).ofLengthBetween(15, 35))
            .field(
                "text",
                strings()
                    .basicLatinAlphabet()
                    .multi((int) (80 * scale))
                    .ofLengthBetween(100, (int) (200 * scale)))
            .field(
                "text2_s",
                strings()
                    .basicLatinAlphabet()
                    .multi((int) (800 * scale))
                    .ofLengthBetween(50, (int) (150 * scale)));

    SolrDocumentList docList = new SolrDocumentList();
    for (int i = 0; i < count; i++) {
      SolrDocument doc = docs.document();
      docList.add(doc);
    }
    docList.setNumFound((long) scale);
    docList.setMaxScore(1.0f);
    docList.setStart(0);

    response.add("docs", docList);

    return response;
  }

  private static Object veryLargeTextAndStrings(int count, float scale) {
    // BaseBenchState.log("count=" + count + ' ' + "scale=" + scale + ' ' + "count * scale=" + count
    // * scale);
    NamedList<Object> response = new NamedList<>();

    NamedList<Object> header = new NamedList<>();
    header.add("status", 0);
    header.add("headerStuff", "values");
    response.add("header", header);

    Docs docs =
        docs()
            .field("id", integers().incrementing())
            .field(
                "facet_s",
                strings()
                    .basicLatinAlphabet()
                    .maxCardinality(5)
                    .ofLengthBetween(50, (int) (64 * scale)))
            .field(
                "facet2_s",
                strings().basicLatinAlphabet().maxCardinality(100).ofLengthBetween(12, 16))
            .field(
                "facet3_s",
                strings().basicLatinAlphabet().maxCardinality(1200).ofLengthBetween(110, 128))
            .field(
                "text",
                strings()
                    .basicLatinAlphabet()
                    .multi((int) (800 * scale))
                    .ofLengthBetween(500, (int) (500 * scale)))
            .field(
                "text2_s",
                strings()
                    .basicLatinAlphabet()
                    .multi((int) (800 * scale))
                    .ofLengthBetween(2000, (int) (1500 * scale)))
            .field(
                "text3_t",
                strings()
                    .basicLatinAlphabet()
                    .multi((int) (800 * scale))
                    .ofLengthBetween(2500, (int) (2000 * scale)))
            .field("int_i", integers().all())
            .field("long1_l", longs().all())
            .field("long2_l", longs().all())
            .field("long3_l", longs().all())
            .field("int2_i", integers().allWithMaxCardinality(500));

    SolrDocumentList docList = new SolrDocumentList();
    for (int i = 0; i < count; i++) {
      SolrDocument doc = docs.document();
      docList.add(doc);
    }
    docList.setNumFound(count);
    docList.setMaxScore(1.0f);
    docList.setStart(0);

    response.add("docs", docList);

    response.add("int", 42);
    response.add("long", 5000_023L);
    response.add("date", new Date());

    return response;
  }
}

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
package org.apache.solr.bench;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * A tool to generate controlled random data for a benchmark. {@link SolrInputDocument}s are created
 * based on supplied FieldDef definitions.
 *
 * <p>You can call getDocument to build and retrieve one {@link SolrInputDocument} at a time, or you
 * can call {@link #preGenerateDocs} to generate the given number of documents in RAM, and then
 * retrieve them via {@link #getGeneratedDocsIterator}.
 */
public class DocMaker {

  private Queue<SolrInputDocument> docs = new ConcurrentLinkedQueue<>();

  private final Map<String, FieldDef> fields = new HashMap<>();

  private static final AtomicInteger ID = new AtomicInteger();

  private ExecutorService executorService;

  public DocMaker() {}

  @SuppressForbidden(reason = "This module does not need to deal with logging context")
  public void preGenerateDocs(int numDocs, SplittableRandom random) throws InterruptedException {
    MiniClusterState.log("preGenerateDocs " + numDocs + " ...");
    docs.clear();
    executorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() + 1,
            new SolrNamedThreadFactory("SolrJMH DocMaker"));

    for (int i = 0; i < numDocs; i++) {
      executorService.submit(
          new Runnable() {
            SplittableRandom threadRandom = random.split();

            @Override
            public void run() {
              try {
                SolrInputDocument doc = DocMaker.this.getDocument(threadRandom);
                docs.add(doc);
              } catch (Exception e) {
                executorService.shutdownNow();
                throw new RuntimeException(e);
              }
            }
          });
    }

    executorService.shutdown();
    boolean result = executorService.awaitTermination(10, TimeUnit.MINUTES);
    if (!result) {
      throw new RuntimeException("Timeout waiting for doc adds to finish");
    }
    MiniClusterState.log(
        "done preGenerateDocs docs="
            + docs.size()
            + " ram="
            + RamUsageEstimator.humanReadableUnits(RamUsageEstimator.sizeOfObject(docs)));

    if (numDocs != docs.size()) {
      throw new IllegalStateException("numDocs != " + docs.size());
    }
  }

  public Iterator<SolrInputDocument> getGeneratedDocsIterator() {
    return docs.iterator();
  }

  public SolrInputDocument getDocument(SplittableRandom random) {
    SolrInputDocument doc = new SolrInputDocument();

    for (Map.Entry<String, FieldDef> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), getValue(entry.getValue(), random));
    }

    return doc;
  }

  public void addField(String name, FieldDef.FieldDefBuilder builder) {
    fields.put(name, builder.build());
  }

  private Object getValue(FieldDef fieldDef, SplittableRandom threadRandom) {

    switch (fieldDef.getContent()) {
      case UNIQUE_INT:
        return ID.incrementAndGet();
      case INTEGER:
        if (fieldDef.getMaxCardinality() > 0) {
          long start = fieldDef.getCardinalityStart();
          long seed = nextLong(start, start + fieldDef.getMaxCardinality(), threadRandom.split());
          return nextInt(0, Integer.MAX_VALUE, new SplittableRandom(seed));
        }

        return threadRandom.nextInt(Integer.MAX_VALUE);
      case ALPHEBETIC:
        return getString(
            fieldDef, value -> getAlphabeticString(fieldDef, threadRandom), threadRandom);
      case UNICODE:
        return getString(fieldDef, value -> getUnicodeString(fieldDef, threadRandom), threadRandom);
      default:
        throw new UnsupportedOperationException(
            "Unsupported content type type=" + fieldDef.getContent());
    }
  }

  private String getString(
      FieldDef fieldDef, StringSupplier supplier, SplittableRandom threadRandom) {
    if (fieldDef.getNumTokens() > 1 || fieldDef.getMaxNumTokens() > 1) {
      StringBuilder sb =
          new StringBuilder(
              fieldDef.getNumTokens()
                  * (Math.max(fieldDef.getLength(), fieldDef.getMaxLength()) + 1));
      SplittableRandom random = threadRandom.split();
      for (int i = 0;
          i
              < (fieldDef.getMaxNumTokens() > 1
                  ? random.nextInt(1, fieldDef.getMaxNumTokens())
                  : fieldDef.getNumTokens());
          i++) {
        if (i > 0) {
          sb.append(' ');
        }
        sb.append(supplier.getString(fieldDef));
      }
      return sb.toString();
    }
    return supplier.getString(fieldDef);
  }

  private String getUnicodeString(FieldDef fieldDef, SplittableRandom threadRandom) {
    try {
      if (fieldDef.getMaxCardinality() > 0) {
        long start = fieldDef.getCardinalityStart();
        long seed = nextLong(start, start + fieldDef.getMaxCardinality(), threadRandom.split());
        if (fieldDef.getLength() > -1) {
          return TestUtil.randomRealisticUnicodeString(
              new Random(seed), fieldDef.getLength(), fieldDef.getLength());
        } else {
          return TestUtil.randomRealisticUnicodeString(
              new Random(seed), 1, fieldDef.getMaxLength());
        }
      }

      if (fieldDef.getLength() > -1) {
        return TestUtil.randomRealisticUnicodeString(
            new Random(threadRandom.nextLong()), fieldDef.getLength(), fieldDef.getLength());
      } else {
        return TestUtil.randomRealisticUnicodeString(
            new Random(threadRandom.nextLong()), 1, fieldDef.getMaxLength());
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed getting UnicodeString with FieldDef=" + fieldDef, e);
    }
  }

  private String getAlphabeticString(FieldDef fieldDef, SplittableRandom threadRandom) {
    try {
      if (fieldDef.getMaxCardinality() > 0) {
        long start = fieldDef.getCardinalityStart();
        long seed = nextLong(start, start + fieldDef.getMaxCardinality(), threadRandom.split());
        SplittableRandom random = new SplittableRandom(seed);
        if (fieldDef.getLength() > -1) {
          return RandomStringUtils.random(
              nextInt(fieldDef.getLength(), fieldDef.getLength(), random),
              0,
              0,
              true,
              false,
              null,
              new Random(seed));
        } else {
          return RandomStringUtils.random(
              nextInt(1, fieldDef.getMaxLength(), random),
              0,
              0,
              true,
              false,
              null,
              new Random(seed));
        }
      }

      SplittableRandom random = threadRandom.split();
      Random r = new Random(random.nextLong());
      if (fieldDef.getLength() > -1) {
        return RandomStringUtils.random(
            nextInt(fieldDef.getLength(), fieldDef.getLength(), random),
            0,
            0,
            true,
            false,
            null,
            r);
      } else {
        return RandomStringUtils.random(
            nextInt(1, fieldDef.getMaxLength(), random), 0, 0, true, false, null, r);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed getting AlphabeticString with FieldDef=" + fieldDef, e);
    }
  }

  public void clear() {
    docs.clear();
  }

  public enum Content {
    UNICODE,
    ALPHEBETIC,
    INTEGER,
    UNIQUE_INT
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DocMaker that = (DocMaker) o;
    return fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  public static int nextInt(
      final int startInclusive, final int endExclusive, SplittableRandom random) {
    Validate.isTrue(
        endExclusive >= startInclusive, "Start value must be smaller or equal to end value.");
    Validate.isTrue(startInclusive >= 0, "Both range values must be non-negative.");

    if (startInclusive == endExclusive) {
      return startInclusive;
    }

    return startInclusive + random.nextInt(endExclusive - startInclusive);
  }

  public static long nextLong(
      final long startInclusive, final long endExclusive, SplittableRandom random) {
    Validate.isTrue(
        endExclusive >= startInclusive, "Start value must be smaller or equal to end value.");
    Validate.isTrue(
        startInclusive >= 0,
        "Both range values must be non-negative startInclusive="
            + startInclusive
            + " endExclusive="
            + endExclusive);

    if (startInclusive == endExclusive) {
      return startInclusive;
    }

    return startInclusive + random.nextLong(endExclusive - startInclusive);
  }

  private interface StringSupplier {
    String getString(FieldDef value);
  }
}

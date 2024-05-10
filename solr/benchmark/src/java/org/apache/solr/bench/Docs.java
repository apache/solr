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

import static org.apache.solr.bench.BaseBenchState.log;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.bench.generators.MultiString;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.BenchmarkRandomSource;

/**
 * A tool to generate controlled random data for a benchmark. {@link SolrInputDocument}s are created
 * based on supplied FieldDef definitions.
 *
 * <p>You can call getDocument to build and retrieve one {@link SolrInputDocument} at a time, or you
 * can call {@link #preGenerate} to generate the given number of documents in RAM, and then retrieve
 * them via {@link #generatedDocsIterator}.
 */
public class Docs {
  private final ThreadLocal<SolrRandomnessSource> random;
  private final Queue<SolrInputDocument> docs = new ConcurrentLinkedQueue<>();

  private final Map<String, Gen<?>> fields =
      Collections.synchronizedMap(CollectionUtil.newHashMap(16));

  private ExecutorService executorService;
  private int stringFields;
  private int multiStringFields;
  private int integerFields;
  private int longFields;
  private int booleanFields;
  private int floatFields;
  private int dateFields;
  private int doubleFields;

  /**
   * Docs docs.
   *
   * @return the docs
   */
  public static Docs docs() {
    return new Docs(BaseBenchState.getRandomSeed());
  }

  /**
   * Docs docs.
   *
   * @param seed the seed
   * @return the docs
   */
  public static Docs docs(Long seed) {
    return new Docs(seed);
  }

  private Docs(Long seed) {
    this.random =
        ThreadLocal.withInitial(
            () ->
                new BenchmarkRandomSource(
                    new SplittableRandomGenerator(seed))); // TODO: pluggable RandomGenerator
  }

  /**
   * Pre generate iterator.
   *
   * @param numDocs the num docs
   * @return the iterator
   * @throws InterruptedException the interrupted exception
   */
  @SuppressForbidden(reason = "This module does not need to deal with logging context")
  public Iterator<SolrInputDocument> preGenerate(int numDocs) throws InterruptedException {
    log("preGenerate docs=" + numDocs + " ...");
    docs.clear();
    executorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() + 1,
            new SolrNamedThreadFactory("SolrJMH DocMaker"));

    for (int i = 0; i < numDocs; i++) {
      executorService.submit(
          () -> {
            docs.add(Docs.this.inputDocument());
          });
    }

    executorService.shutdown();
    boolean result = executorService.awaitTermination(10, TimeUnit.MINUTES);
    if (!result) {
      throw new RuntimeException("Timeout waiting for doc adds to finish");
    }
    log(
        "done preGenerateDocs docs="
            + docs.size()
            + " ram="
            + RamUsageEstimator.humanReadableUnits(RamUsageEstimator.sizeOfObject(docs)));

    if (numDocs != docs.size()) {
      throw new IllegalStateException("numDocs != " + docs.size());
    }

    return docs.iterator();
  }

  /**
   * Generated docs iterator.
   *
   * @return the iterator
   */
  public Iterator<SolrInputDocument> generatedDocsIterator() {
    return docs.iterator();
  }

  /**
   * Generated docs circular iterator.
   *
   * @return the iterator that never ends
   */
  public CircularIterator<SolrInputDocument> generatedDocsCircularIterator() {
    return new CircularIterator<>(docs);
  }

  /**
   * Input document solr input document.
   *
   * @return the solr input document
   */
  public SolrInputDocument inputDocument() {
    SolrInputDocument doc = new SolrInputDocument();
    SolrRandomnessSource randomSource = random.get();
    for (Map.Entry<String, Gen<?>> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue().generate(randomSource));
    }

    return doc;
  }

  /**
   * Document solr document.
   *
   * @return the solr document
   */
  public SolrDocument document() {
    SolrDocument doc = new SolrDocument();
    SolrRandomnessSource randomSource = random.get();
    for (Map.Entry<String, Gen<?>> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue().generate(randomSource));
    }

    return doc;
  }

  /**
   * Field docs.
   *
   * @param name the name
   * @param generator the generator
   * @return the docs
   */
  public Docs field(String name, Gen<?> generator) {
    fields.put(name, generator);
    return this;
  }

  /**
   * Field docs.
   *
   * @param generator the generator
   * @return the docs
   */
  public Docs field(SolrGen<?> generator) {
    Class<?> type = generator.type();
    if (String.class == type) {
      fields.put("string" + (stringFields++ > 0 ? stringFields : "") + "_s", generator);
    } else if (MultiString.class == type) {
      fields.put("text" + (multiStringFields++ > 0 ? multiStringFields : "") + "_t", generator);
    } else if (Integer.class == type) {
      fields.put("int" + (integerFields++ > 0 ? integerFields : "") + "_i", generator);
    } else if (Long.class == type) {
      fields.put("long" + (longFields++ > 0 ? longFields : "") + "_l", generator);
    } else if (Boolean.class == type) {
      fields.put("boolean" + (booleanFields++ > 0 ? booleanFields : "") + "_b", generator);
    } else if (Float.class == type) {
      fields.put("float" + (floatFields++ > 0 ? floatFields : "") + "_f", generator);
    } else if (Date.class == type) {
      fields.put("date" + (dateFields++ > 0 ? dateFields : "") + "_dt", generator);
    } else if (Double.class == type) {
      fields.put("double" + (doubleFields++ > 0 ? doubleFields : "") + "_d", generator);
    } else {
      throw new IllegalArgumentException("Unknown type: " + generator.type());
    }

    return this;
  }

  /** Clear. */
  public void clear() {
    docs.clear();
  }
}

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
package org.apache.solr.bench.generators;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.solr.bench.SolrRandomnessSource;
import org.jctools.maps.NonBlockingHashMap;
import org.quicktheories.api.AsString;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.BenchmarkRandomSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Solr gen.
 *
 * @param <T> the type parameter
 */
public class SolrGen<T> implements Gen<T> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** The constant OPEN_PAREN. */
  public static final String OPEN_PAREN = " (";

  /** The constant CLOSE_PAREN. */
  public static final char CLOSE_PAREN = ')';

  /** The constant COUNT_TYPES_ARE_TRACKED_LIMIT_WAS_REACHED. */
  public static final String COUNT_TYPES_ARE_TRACKED_LIMIT_WAS_REACHED =
      " Count types are tracked, limit was reached.\n\n";

  /** The constant RANDOM_DATA_GEN_REPORTS. */
  public static final String RANDOM_DATA_GEN_REPORTS =
      "\n\n\n*****  Random Data Gen Reports *****\n\n";

  /** The constant ONLY. */
  public static final String ONLY = "\n\nOnly ";

  /** The Child. */
  protected final Gen<T> child;

  private final Class<?> type;
  private Distribution distribution = Distribution.UNIFORM;

  /** The Start. */
  protected long start;

  /** The End. */
  protected long end;

  private static final boolean COLLECT_COUNTS = Boolean.getBoolean("random.counts");

  static {
    log.info("random.counts is set to {}", COLLECT_COUNTS);
  }

  private String description;

  private String collectKey;
  /** The constant COUNTS. */
  public static final Map<String, RandomDataHistogram.Counts> COUNTS = new NonBlockingHashMap<>(64);

  /**
   * Counts report list.
   *
   * @return the list
   */
  public static List<String> countsReport() {
    List<String> reports = new ArrayList<>(COUNTS.size());
    reports.add(RANDOM_DATA_GEN_REPORTS);
    if (COUNTS.size() >= RandomDataHistogram.MAX_TYPES_TO_COLLECT) {
      reports.add(
          ONLY
              + RandomDataHistogram.MAX_TYPES_TO_COLLECT
              + COUNT_TYPES_ARE_TRACKED_LIMIT_WAS_REACHED);
    }
    COUNTS.forEach(
        (k, v) -> {
          reports.add(v.print());
        });
    return reports;
  }

  /** Instantiates a new Solr gen. */
  protected SolrGen() {
    child = null;
    this.type = null;
  }

  /**
   * Instantiates a new Solr gen.
   *
   * @param child the child
   * @param type the type
   */
  public SolrGen(Gen<T> child, Class<?> type) {
    this.child = child;
    this.type = type;
    if (type != null) {
      this.description = type.getSimpleName();
    } else if (child instanceof SolrGen) {
      this.description = ((SolrGen<?>) child).description;
    }
    if (COLLECT_COUNTS && description != null) {
      this.collectKey = description + OPEN_PAREN + distribution + CLOSE_PAREN;
    }
  }

  /**
   * Instantiates a new Solr gen.
   *
   * @param type the type
   */
  SolrGen(Class<?> type) {
    child = null;
    this.type = type;
    this.description = type.getSimpleName();
    if (COLLECT_COUNTS) {
      this.collectKey = description + OPEN_PAREN + distribution + CLOSE_PAREN;
    }
  }

  /**
   * Instantiates a new Solr gen.
   *
   * @param child the child
   */
  protected SolrGen(Gen<T> child) {
    this.child = child;
    this.type = null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T generate(RandomnessSource in) {
    T val;
    if (child == null && start == end) {
      val = generate((SolrRandomnessSource) in);
      processCounts(val);
      return val;
    }

    if (child == null) {
      val = (T) Integer.valueOf((int) ((SolrRandomnessSource) in).next(start, end));
      processCounts(val);
      return val;
    }

    if (child instanceof SolrGen && in instanceof SolrRandomnessSource) {
      val =
          (T)
              ((SolrGen<Object>) child)
                  .generate(((SolrRandomnessSource) in).withDistribution(distribution));
      processCounts(val);
      return val;
    }

    if (in instanceof BenchmarkRandomSource) {
      val = child.generate(((BenchmarkRandomSource) in).withDistribution(distribution));
      processCounts(val);
      return val;
    }
    val = child.generate(in);
    processCounts(val);
    return val;
  }

  private void processCounts(T val) {
    if (COLLECT_COUNTS) {

      if (collectKey == null || COUNTS.size() > RandomDataHistogram.MAX_TYPES_TO_COLLECT) {
        return;
      }

      // System.out.println("Add key " + key);
      RandomDataHistogram.Counts newCounts = null;
      RandomDataHistogram.Counts counts;
      counts = COUNTS.get(collectKey);

      if (counts == null) {
        newCounts =
            new RandomDataHistogram.Counts(
                collectKey,
                type != null
                    && Number.class.isAssignableFrom(type)
                    && (type != Double.class && type != Float.class));
        counts = COUNTS.putIfAbsent(collectKey, newCounts);
      }

      if (counts == null) {
        newCounts.collect(val);
      } else {
        counts.collect(val);
      }
    }
  }

  /**
   * Generate t.
   *
   * @param in the in
   * @return the t
   */
  @SuppressWarnings("unchecked")
  public T generate(SolrRandomnessSource in) {
    if (child == null && start == end) {
      T val = generate((RandomnessSource) in);
      processCounts(val);
      return val;
    }

    if (child == null) {
      T val = (T) Integer.valueOf((int) in.next(start, end));
      processCounts(val);
      return val;
    }

    T val = child.generate(in);
    processCounts(val);
    return val;
  }

  /**
   * Type class.
   *
   * @return the class
   */
  @SuppressWarnings("unchecked")
  public Class<?> type() {
    SolrGen<?> other = this;
    while (true) {
      if (other.type == null && other.child instanceof SolrGen) {
        other = ((SolrGen<Object>) other.child);
        continue;
      }
      return other.type;
    }
  }

  /**
   * Described as solr gen.
   *
   * @param description the description
   * @return the solr gen
   */
  public SolrGen<T> describedAs(String description) {
    this.description = description;
    if (COLLECT_COUNTS) {
      this.collectKey = description + OPEN_PAREN + distribution + CLOSE_PAREN;
    }
    return this;
  }

  @Override
  public SolrGen<T> describedAs(AsString<T> asString) {
    return new SolrDescribingGenerator<>(this, asString);
  }

  /**
   * Mix solr gen.
   *
   * @param rhs the rhs
   * @param type the type
   * @return the solr gen
   */
  public SolrGen<T> mix(Gen<T> rhs, Class<?> type) {
    return mix(rhs, 50, type);
  }

  @Override
  public Gen<T> mix(Gen<T> rhs, int weight) {
    return mix(rhs, weight, null);
  }

  /**
   * Mix solr gen.
   *
   * @param rhs the rhs
   * @param weight the weight
   * @param type the type
   * @return the solr gen
   */
  public SolrGen<T> mix(Gen<T> rhs, int weight, Class<?> type) {
    return new SolrGen<>(type) {
      @Override
      public T generate(SolrRandomnessSource in) {
        while (true) {
          long picked = in.next(0, 99);
          if (picked >= weight) {
            continue;
          }
          return ((SolrGen<T>) rhs).generate(in);
        }
      }
    };
  }

  /**
   * Flat maps generated values with supplied function
   *
   * @param mapper function to map with
   * @param <R> Type to map to
   * @return A Gen of R
   */
  @Override
  public <R> Gen<R> flatMap(Function<? super T, Gen<? extends R>> mapper) {
    return new SolrGen<>(in -> mapper.apply(generate(in)).generate(in), null);
  }

  @Override
  public <R> Gen<R> map(Function<? super T, ? extends R> mapper) {
    return map(mapper, null);
  }

  /**
   * Map solr gen.
   *
   * @param <R> the type parameter
   * @param mapper the mapper
   * @param type the type
   * @return the solr gen
   */
  public <R> SolrGen<R> map(Function<? super T, ? extends R> mapper, Class<?> type) {
    return new SolrGen<>(in -> mapper.apply(generate(in)), type);
  }

  /**
   * With distribution solr gen.
   *
   * @param distribution the distribution
   * @return the solr gen
   */
  public SolrGen<T> withDistribution(Distribution distribution) {
    this.distribution = distribution;
    if (this.child instanceof SolrGen) {
      ((SolrGen<?>) this.child).distribution = distribution;
    }
    if (COLLECT_COUNTS) {
      this.collectKey = description + OPEN_PAREN + distribution + CLOSE_PAREN;
    }
    return this;
  }

  @Override
  public String toString() {
    return "SolrGen{"
        + "child="
        + child
        + ", type="
        + type
        + ", distribution="
        + distribution
        + '}';
  }

  /**
   * Gets distribution.
   *
   * @return the distribution
   */
  protected Distribution getDistribution() {
    return this.distribution;
  }
}

/**
 * The type Solr describing generator.
 *
 * @param <G> the type parameter
 */
class SolrDescribingGenerator<G> extends SolrGen<G> {

  private final AsString<G> toString;

  /**
   * Instantiates a new Solr describing generator.
   *
   * @param child the child
   * @param toString the to string
   */
  public SolrDescribingGenerator(Gen<G> child, AsString<G> toString) {
    super(child);
    this.toString = toString;
  }

  @Override
  public G generate(RandomnessSource in) {
    G val;
    val = Objects.requireNonNull(child).generate(in);
    return val;
  }

  @Override
  public G generate(SolrRandomnessSource in) {
    G val;
    val = Objects.requireNonNull(child).generate(in);
    return val;
  }

  @Override
  public String asString(G t) {
    return toString.asString(t);
  }
}

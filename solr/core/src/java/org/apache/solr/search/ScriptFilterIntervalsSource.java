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
package org.apache.solr.search;

import java.io.IOException;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.intervals.FilteredIntervalsSource;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

/**
 * An {@link FilteredIntervalsSource} that filters intervals by evaluating a Lucene Expressions
 * script against the current interval.
 *
 * <p>The script is bound to the pseudo-variables {@code interval.start}, {@code interval.end} and
 * {@code interval.gap}, and an interval is accepted when the script evaluates to a non-zero value.
 */
public class ScriptFilterIntervalsSource extends FilteredIntervalsSource {

  /** Bindings that expose the current interval's {@code start}, {@code end} and {@code gap}. */
  public static class IntervalBindings extends Bindings {

    private static final DoubleValuesSource START =
        new IntervalDoubleValuesSource("start") {
          @Override
          protected double value(IntervalIterator it) {
            return it.start();
          }
        };
    private static final DoubleValuesSource END =
        new IntervalDoubleValuesSource("end") {
          @Override
          protected double value(IntervalIterator it) {
            return it.end();
          }
        };
    private static final DoubleValuesSource GAP =
        new IntervalDoubleValuesSource("gap") {
          @Override
          protected double value(IntervalIterator it) {
            return it.gaps();
          }
        };

    @Override
    public DoubleValuesSource getDoubleValuesSource(String name) {
      return switch (name) {
        case "interval.start" -> START;
        case "interval.end" -> END;
        case "interval.gap" -> GAP;
        default ->
            throw new IllegalArgumentException(
                "Unknown interval variable '"
                    + name
                    + "'. Available: interval.start, interval.end, interval.gap");
      };
    }
  }

  /** A {@link DoubleValuesSource} that reads a scalar property of the current interval. */
  private abstract static class IntervalDoubleValuesSource extends DoubleValuesSource {

    private final String variable;

    IntervalDoubleValuesSource(String variable) {
      this.variable = variable;
    }

    @Override
    public final DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
        throws IOException {
      throw new IOException(
          "interval variable '" + variable + "' can only be evaluated within an interval filter");
    }

    @Override
    public final boolean needsScores() {
      return false;
    }

    @Override
    public final boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public final DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public final int hashCode() {
      return variable.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
      return obj instanceof IntervalDoubleValuesSource that && variable.equals(that.variable);
    }

    @Override
    public final String toString() {
      return "interval." + variable;
    }

    protected abstract double value(IntervalIterator it);
  }

  private final Expression expression;
  private final IntervalBindings bindings;

  /**
   * Creates a script-filtered intervals source.
   *
   * @param in the source to filter
   * @param expression a compiled expression whose variables are resolved through {@code bindings}
   * @param bindings bindings exposing the interval pseudo-variables
   */
  public ScriptFilterIntervalsSource(
      IntervalsSource in, Expression expression, IntervalBindings bindings) {
    super("SCRIPT(" + expression.sourceText + ")", in);
    this.expression = expression;
    this.bindings = bindings;
  }

  @Override
  protected boolean accept(IntervalIterator it) {
    try {
      DoubleValues[] functionValues = new DoubleValues[expression.variables.length];
      for (int i = 0; i < functionValues.length; i++) {
        DoubleValuesSource source = bindings.getDoubleValuesSource(expression.variables[i]);
        functionValues[i] =
            new DoubleValues() {
              @Override
              public double doubleValue() {
                return source instanceof IntervalDoubleValuesSource ids
                    ? ids.value(it)
                    : Double.NaN;
              }

              @Override
              public boolean advanceExact(int doc) {
                return true;
              }
            };
      }
      return expression.evaluate(functionValues) != 0;
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to evaluate interval script '" + expression.sourceText + "'", e);
    }
  }
}

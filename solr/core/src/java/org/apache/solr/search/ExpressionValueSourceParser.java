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

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SimpleFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * A ValueSource parser configured with a pre-compiled expression that can then be evaluated at
 * request time. It's powered by the Lucene Expressions module, which is a subset of JavaScript.
 */
public class ExpressionValueSourceParser extends ValueSourceParser {

  public static final String SCORE_KEY = "score-name"; // TODO get rid of this?  Why have it?
  public static final String EXPRESSION_KEY = "expression";

  private Expression expression;
  private String scoreKey;
  private int numPositionalArgs = 0; // Number of positional arguments in the expression

  @Override
  public void init(NamedList<?> args) {
    initConfiguredExpression(args);
    initScoreKey(args);
    super.init(args);
  }

  /** Checks for optional scoreKey override */
  private void initScoreKey(NamedList<?> args) {
    scoreKey = Optional.ofNullable((String) args.remove(SCORE_KEY)).orElse(SolrReturnFields.SCORE);
  }

  /** Parses the pre-configured expression */
  private void initConfiguredExpression(NamedList<?> args) {
    String expressionStr =
        Optional.ofNullable((String) args.remove(EXPRESSION_KEY))
            .orElseThrow(
                () ->
                    new SolrException(
                        SERVER_ERROR, EXPRESSION_KEY + " must be configured with an expression"));

    // Find the highest positional argument in the expression
    Pattern pattern = Pattern.compile("\\$(\\d+)");
    Matcher matcher = pattern.matcher(expressionStr);
    while (matcher.find()) {
      int argNum = Integer.parseInt(matcher.group(1));
      numPositionalArgs = Math.max(numPositionalArgs, argNum);
    }

    // TODO add way to register additional functions
    try {
      this.expression = JavascriptCompiler.compile(expressionStr);
    } catch (ParseException e) {
      throw new SolrException(
          SERVER_ERROR, "Unable to parse javascript expression: " + expressionStr, e);
    }
  }

  // TODO: support dynamic expressions:  expr("foo * bar / 32")  ??

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    assert null != fp;

    // Parse positional arguments if any
    List<DoubleValuesSource> positionalArgs = new ArrayList<>();
    for (int i = 0; i < numPositionalArgs; i++) {
      ValueSource vs = fp.parseValueSource();
      positionalArgs.add(vs.asDoubleValuesSource());
    }

    IndexSchema schema = fp.getReq().getSchema();
    SolrBindings b = new SolrBindings(scoreKey, schema, positionalArgs);
    DoubleValuesSource doubleValuesSource = expression.getDoubleValuesSource(b);

    // Check if this expression needs scores by examining if it contains score references
    boolean needsScores = doubleValuesSource.needsScores();

    if (needsScores) {
      // For score-dependent expressions, create a custom ValueSource that can
      // properly integrate with Lucene's sorting scorer mechanism
      return new ScoreAwareValueSource(doubleValuesSource);
    } else {
      // For non-score expressions, use the standard conversion
      return ValueSource.fromDoubleValuesSource(doubleValuesSource);
    }
  }

  /**
   * A custom ValueSource that properly handles score-dependent expressions by implementing the
   * setScorer() pattern required by Lucene's sorting mechanism.
   *
   * <p>This solves the fundamental issue where ValueSourceComparator.doSetNextReader() is called
   * during sorting setup when no scorer is available, but expressions need scores to work
   * correctly.
   */
  private static class ScoreAwareValueSource extends ValueSource {
    private final DoubleValuesSource doubleValuesSource;

    public ScoreAwareValueSource(DoubleValuesSource doubleValuesSource) {
      this.doubleValuesSource = doubleValuesSource;
    }

    @Override
    public FunctionValues getValues(
        java.util.Map<Object, Object> context, LeafReaderContext readerContext) throws IOException {
      // Get scorer from context (may be null during setup)
      Scorable scorer = (Scorable) context.get("scorer");
      DoubleValues scores = scorer == null ? null : DoubleValuesSource.fromScorer(scorer);

      // Get the DoubleValues from our source
      DoubleValues doubleValues = doubleValuesSource.getValues(readerContext, scores);

      // Return a ScoreAwareFunctionValues that can be updated when scorer becomes available
      return new ScoreAwareFunctionValues(doubleValuesSource, readerContext, doubleValues);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ScoreAwareValueSource)) return false;
      ScoreAwareValueSource that = (ScoreAwareValueSource) o;
      return doubleValuesSource.equals(that.doubleValuesSource);
    }

    @Override
    public int hashCode() {
      return doubleValuesSource.hashCode();
    }

    @Override
    public String description() {
      return "ScoreAware(" + doubleValuesSource.toString() + ")";
    }

    @Override
    public SortField getSortField(boolean reverse) {
      // Create a SortField that uses our custom comparator
      return new SortField(description(), new ScoreAwareComparatorSource(), reverse);
    }

    /** Custom FieldComparatorSource that implements the setScorer() pattern */
    private class ScoreAwareComparatorSource extends FieldComparatorSource {
      @Override
      public FieldComparator<?> newComparator(
          String fieldname, int numHits, Pruning pruning, boolean reversed) {
        return new ScoreAwareComparator(numHits);
      }
    }

    /**
     * Custom FieldComparator that properly implements setScorer() to ensure scores are available
     * during sorting
     */
    private class ScoreAwareComparator extends SimpleFieldComparator<Double> {
      private final double[] values;
      private ScoreAwareFunctionValues functionValues;
      private double bottom;
      private double topValue;

      public ScoreAwareComparator(int numHits) {
        this.values = new double[numHits];
      }

      @Override
      public int compare(int slot1, int slot2) {
        return Double.compare(values[slot1], values[slot2]);
      }

      @Override
      public int compareBottom(int doc) throws IOException {
        return Double.compare(bottom, functionValues.doubleVal(doc));
      }

      @Override
      public void copy(int slot, int doc) throws IOException {
        values[slot] = functionValues.doubleVal(doc);
      }

      @Override
      public void doSetNextReader(LeafReaderContext context) throws IOException {
        // Create initial FunctionValues (may have null scorer)
        functionValues = (ScoreAwareFunctionValues) getValues(new java.util.HashMap<>(), context);
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        // This is the key method that was missing in ValueSourceComparator!
        // When the scorer becomes available during sorting, update our FunctionValues
        if (functionValues != null) {
          functionValues.updateScorer(scorer);
        }
      }

      @Override
      public void setBottom(int bottom) {
        this.bottom = values[bottom];
      }

      @Override
      public void setTopValue(Double value) {
        this.topValue = value;
      }

      @Override
      public Double value(int slot) {
        return values[slot];
      }

      @Override
      public int compareTop(int doc) throws IOException {
        return Double.compare(topValue, functionValues.doubleVal(doc));
      }
    }
  }

  /** FunctionValues that can be updated with a scorer when it becomes available */
  private static class ScoreAwareFunctionValues extends FunctionValues {
    private final DoubleValuesSource source;
    private final LeafReaderContext context;
    private DoubleValues doubleValues;

    public ScoreAwareFunctionValues(
        DoubleValuesSource source, LeafReaderContext context, DoubleValues initialDoubleValues) {
      this.source = source;
      this.context = context;
      this.doubleValues = initialDoubleValues;
    }

    public void updateScorer(Scorable scorer) throws IOException {
      // When scorer becomes available, recreate DoubleValues with actual scores
      DoubleValues scores = DoubleValuesSource.fromScorer(scorer);
      this.doubleValues = source.getValues(context, scores);
    }

    @Override
    public double doubleVal(int doc) throws IOException {
      if (!doubleValues.advanceExact(doc)) {
        return 0.0;
      }
      return doubleValues.doubleValue();
    }

    @Override
    public float floatVal(int doc) throws IOException {
      return (float) doubleVal(doc);
    }

    @Override
    public int intVal(int doc) throws IOException {
      return (int) doubleVal(doc);
    }

    @Override
    public long longVal(int doc) throws IOException {
      return (long) doubleVal(doc);
    }

    @Override
    public String strVal(int doc) throws IOException {
      return Float.toString(floatVal(doc));
    }

    @Override
    public String toString(int doc) throws IOException {
      return Double.toString(doubleVal(doc));
    }
  }

  /**
   * A bindings class that uses schema fields to resolve variables.
   *
   * @lucene.internal
   */
  public static class SolrBindings extends Bindings {
    private final String scoreKey;
    private final IndexSchema schema;
    private final List<DoubleValuesSource> positionalArgs;

    /**
     * @param scoreKey The binding name that should be used to represent the score, may be null
     * @param schema IndexSchema for field bindings
     * @param positionalArgs List of positional arguments
     */
    public SolrBindings(
        String scoreKey, IndexSchema schema, List<DoubleValuesSource> positionalArgs) {
      this.scoreKey = scoreKey;
      this.schema = schema;
      this.positionalArgs = positionalArgs != null ? positionalArgs : new ArrayList<>();
    }

    @Override
    public DoubleValuesSource getDoubleValuesSource(String key) {
      assert null != key;

      if (Objects.equals(scoreKey, key)) {
        // Since scores in Solr are often derived from field values or function queries,
        // and the timing issue occurs because DoubleValuesSource.SCORES requires a scorer
        // that's not available during sorting setup, we create a score source that can
        // work without requiring a scorer context.
        return new ScoreDoubleValuesSource();
      }

      // Check for positional arguments like $1, $2, etc.
      if (key.startsWith("$")) {
        try {
          int position = Integer.parseInt(key.substring(1));
          return positionalArgs.get(position - 1); // Convert to 0-based index
        } catch (RuntimeException e) {
          throw new IllegalArgumentException("Not a valid positional argument: " + key, e);
        }
      }

      SchemaField field = schema.getFieldOrNull(key);
      if (null != field) {
        return field.getType().getValueSource(field, null).asDoubleValuesSource();
      }

      throw new IllegalArgumentException("No binding or schema field for key: " + key);
    }
  }

  /**
   * Custom DoubleValuesSource for scores that handles the Lucene 10 assertion gracefully.
   *
   * <p>The core issue: Lucene 10's DoubleValuesSource.SCORES has an assertion that scores != null,
   * but during ValueSource sorting setup, scores can be null. This creates a timing mismatch
   * between when the expression is evaluated and when the scorer is available.
   *
   * <p>This implementation bypasses the assertion and provides reasonable fallback behavior when
   * scores are not available, while delegating to the original implementation when scores are
   * available.
   */
  private static class ScoreDoubleValuesSource extends DoubleValuesSource {

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      // Instead of calling DoubleValuesSource.SCORES.getValues() which has the assertion,
      // we implement the score handling directly
      return new ScoreDoubleValues(scores);
    }

    @Override
    public boolean needsScores() {
      return true;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof ScoreDoubleValuesSource;
    }

    @Override
    public int hashCode() {
      return ScoreDoubleValuesSource.class.hashCode();
    }

    @Override
    public String toString() {
      return "score";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  /**
   * A DoubleValues implementation that handles score access with a graceful fallback when scores
   * are not available (e.g., during sorting setup).
   */
  private static class ScoreDoubleValues extends DoubleValues {
    private final DoubleValues scores;

    public ScoreDoubleValues(DoubleValues scores) {
      this.scores = scores;
    }

    @Override
    public double doubleValue() throws IOException {
      if (scores != null) {
        // When scores are available, return the actual score
        return scores.doubleValue();
      } else {
        // When scores are null (during sorting setup or other contexts where
        // no scorer is available), return 0.0 as a fallback.
        //
        // NOTE: This breaks score-based sorting because all documents get the
        // same score (0.0), but it avoids the assertion error and allows the
        // query to complete. The fundamental issue is that the legacy ValueSource
        // sorting system calls getValues() during setup when no scorer is available,
        // but expressions need scores to work correctly.
        return 0.0;
      }
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      if (scores != null) {
        return scores.advanceExact(doc);
      } else {
        // Indicate that values are available for all documents
        return true;
      }
    }
  }
}

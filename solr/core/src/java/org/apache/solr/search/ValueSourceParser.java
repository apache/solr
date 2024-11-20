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
import java.util.Date;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.BoolDocValues;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.queries.function.valuesource.ConstNumberSource;
import org.apache.lucene.queries.function.valuesource.DualFloatFunction;
import org.apache.lucene.queries.function.valuesource.SingleFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * A factory that parses user queries to generate ValueSource instances. Intended usage is to create
 * pluggable, named functions for use in function queries.
 */
public abstract class ValueSourceParser implements NamedListInitializedPlugin {
  /** Parse the user input into a ValueSource. */
  public abstract ValueSource parse(FunctionQParser fp) throws SyntaxError;

  /**
   * standard functions supported by default
   *
   * @deprecated Use {@link ValueSourceParsers#standardValueSourceParsers} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  public static final Map<String, ValueSourceParser> standardValueSourceParsers =
      ValueSourceParsers.standardValueSourceParsers;

  /**
   * @deprecated Use {@link ValueSourceParsers.DateValueSourceParser} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  static class DateValueSourceParser extends ValueSourceParser {
    public Date getDate(FunctionQParser fp, String arg) {
      if (arg == null) return null;
      // check character index 1 to be a digit.  Index 0 might be a +/-.
      if (arg.startsWith("NOW") || (arg.length() > 1 && Character.isDigit(arg.charAt(1)))) {
        Date now = null; // TODO pull from params?
        return DateMathParser.parseMath(now, arg);
      }
      return null;
    }

    public ValueSource getValueSource(FunctionQParser fp, String arg) {
      if (arg == null) return null;
      SchemaField f = fp.req.getSchema().getField(arg);
      return f.getType().getValueSource(f, fp);
    }

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      String first = fp.parseArg();
      String second = fp.parseArg();
      if (first == null) first = "NOW";

      Date d1 = getDate(fp, first);
      ValueSource v1 = d1 == null ? getValueSource(fp, first) : null;

      Date d2 = getDate(fp, second);
      ValueSource v2 = d2 == null ? getValueSource(fp, second) : null;

      // d     constant
      // v     field
      // dd    constant
      // dv    subtract field from constant
      // vd    subtract constant from field
      // vv    subtract fields

      final long ms1 = (d1 == null) ? 0 : d1.getTime();
      final long ms2 = (d2 == null) ? 0 : d2.getTime();

      // "d,dd" handle both constant cases

      if (d1 != null && v2 == null) {
        return new LongConstValueSource(ms1 - ms2);
      }

      // "v" just the date field
      if (v1 != null && v2 == null && d2 == null) {
        return v1;
      }

      // "dv"
      if (d1 != null && v2 != null)
        return new DualFloatFunction(new LongConstValueSource(ms1), v2) {
          @Override
          protected String name() {
            return "ms";
          }

          @Override
          protected float func(int doc, FunctionValues aVals, FunctionValues bVals)
              throws IOException {
            return ms1 - bVals.longVal(doc);
          }
        };

      // "vd"
      if (v1 != null && d2 != null)
        return new DualFloatFunction(v1, new LongConstValueSource(ms2)) {
          @Override
          protected String name() {
            return "ms";
          }

          @Override
          protected float func(int doc, FunctionValues aVals, FunctionValues bVals)
              throws IOException {
            return aVals.longVal(doc) - ms2;
          }
        };

      // "vv"
      if (v1 != null && v2 != null)
        return new DualFloatFunction(v1, v2) {
          @Override
          protected String name() {
            return "ms";
          }

          @Override
          protected float func(int doc, FunctionValues aVals, FunctionValues bVals)
              throws IOException {
            return aVals.longVal(doc) - bVals.longVal(doc);
          }
        };

      return null; // shouldn't happen
    }
  }

  /**
   * @deprecated Use {@link ValueSourceParsers.LongConstValueSource} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  // Private for now - we need to revisit how to handle typing in function queries
  static class LongConstValueSource extends ConstNumberSource {
    final long constant;
    final double dv;
    final float fv;

    public LongConstValueSource(long constant) {
      this.constant = constant;
      this.dv = constant;
      this.fv = constant;
    }

    @Override
    public String description() {
      return "const(" + constant + ")";
    }

    @Override
    public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
        throws IOException {
      return new LongDocValues(this) {
        @Override
        public float floatVal(int doc) {
          return fv;
        }

        @Override
        public int intVal(int doc) {
          return (int) constant;
        }

        @Override
        public long longVal(int doc) {
          return constant;
        }

        @Override
        public double doubleVal(int doc) {
          return dv;
        }

        @Override
        public String toString(int doc) {
          return description();
        }
      };
    }

    @Override
    public int hashCode() {
      return (int) constant + (int) (constant >>> 32);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof LongConstValueSource other)) return false;
      return this.constant == other.constant;
    }

    @Override
    public int getInt() {
      return (int) constant;
    }

    @Override
    public long getLong() {
      return constant;
    }

    @Override
    public float getFloat() {
      return fv;
    }

    @Override
    public double getDouble() {
      return dv;
    }

    @Override
    public Number getNumber() {
      return constant;
    }

    @Override
    public boolean getBool() {
      return constant != 0;
    }
  }

  /**
   * @deprecated Use {@link ValueSourceParsers.NamedParser} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  abstract static class NamedParser extends ValueSourceParser {
    private final String name;

    public NamedParser(String name) {
      this.name = name;
    }

    public String name() {
      return name;
    }
  }

  /**
   * @deprecated Use {@link ValueSourceParsers.DoubleParser} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  abstract static class DoubleParser extends NamedParser {
    public DoubleParser(String name) {
      super(name);
    }

    public abstract double func(int doc, FunctionValues vals) throws IOException;

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      return new Function(fp.parseValueSource());
    }

    class Function extends SingleFunction {
      public Function(ValueSource source) {
        super(source);
      }

      @Override
      public String name() {
        return DoubleParser.this.name();
      }

      @Override
      public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
          throws IOException {
        final FunctionValues vals = source.getValues(context, readerContext);
        return new DoubleDocValues(this) {
          @Override
          public double doubleVal(int doc) throws IOException {
            return func(doc, vals);
          }

          @Override
          public String toString(int doc) throws IOException {
            return name() + '(' + vals.toString(doc) + ')';
          }
        };
      }
    }
  }

  /**
   * @deprecated Use {@link ValueSourceParsers.Double2Parser} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  abstract static class Double2Parser extends NamedParser {
    public Double2Parser(String name) {
      super(name);
    }

    public abstract double func(int doc, FunctionValues a, FunctionValues b) throws IOException;

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      return new Function(fp.parseValueSource(), fp.parseValueSource());
    }

    class Function extends ValueSource {
      private final ValueSource a;
      private final ValueSource b;

      /**
       * @param a the base.
       * @param b the exponent.
       */
      public Function(ValueSource a, ValueSource b) {
        this.a = a;
        this.b = b;
      }

      @Override
      public String description() {
        return name() + "(" + a.description() + "," + b.description() + ")";
      }

      @Override
      public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
          throws IOException {
        final FunctionValues aVals = a.getValues(context, readerContext);
        final FunctionValues bVals = b.getValues(context, readerContext);
        return new DoubleDocValues(this) {
          @Override
          public double doubleVal(int doc) throws IOException {
            return func(doc, aVals, bVals);
          }

          @Override
          public String toString(int doc) throws IOException {
            return name() + '(' + aVals.toString(doc) + ',' + bVals.toString(doc) + ')';
          }
        };
      }

      @Override
      public void createWeight(Map<Object, Object> context, IndexSearcher searcher)
          throws IOException {}

      @Override
      public int hashCode() {
        int h = a.hashCode();
        h ^= (h << 13) | (h >>> 20);
        h += b.hashCode();
        h ^= (h << 23) | (h >>> 10);
        h += name().hashCode();
        return h;
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof Function other)) return false;
        return this.a.equals(other.a) && this.b.equals(other.b);
      }
    }
  }

  /**
   * @deprecated Use {@link ValueSourceParsers.BoolConstValueSource} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  static class BoolConstValueSource extends ConstNumberSource {
    public static final BoolConstValueSource TRUE = new BoolConstValueSource(true);
    public static final BoolConstValueSource FALSE = new BoolConstValueSource(false);

    final boolean constant;

    private BoolConstValueSource(boolean constant) {
      this.constant = constant;
    }

    @Override
    public String description() {
      return "const(" + constant + ")";
    }

    @Override
    public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
        throws IOException {
      return new BoolDocValues(this) {
        @Override
        public boolean boolVal(int doc) {
          return constant;
        }
      };
    }

    @Override
    public int hashCode() {
      return constant ? 0x12345678 : 0x87654321;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof BoolConstValueSource other)) return false;
      return this.constant == other.constant;
    }

    @Override
    public int getInt() {
      return constant ? 1 : 0;
    }

    @Override
    public long getLong() {
      return constant ? 1 : 0;
    }

    @Override
    public float getFloat() {
      return constant ? 1 : 0;
    }

    @Override
    public double getDouble() {
      return constant ? 1 : 0;
    }

    @Override
    public Number getNumber() {
      return constant ? 1 : 0;
    }

    @Override
    public boolean getBool() {
      return constant;
    }
  }

  /**
   * @deprecated Use {@link ValueSourceParsers.TestValueSource} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  static class TestValueSource extends ValueSource {
    ValueSource source;

    public TestValueSource(ValueSource source) {
      this.source = source;
    }

    @Override
    public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
        throws IOException {
      if (context.get(this) == null) {
        SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "testfunc: unweighted value source detected.  delegate="
                + source
                + " request="
                + (requestInfo == null ? "null" : requestInfo.getReq()));
      }
      return source.getValues(context, readerContext);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof TestValueSource && source.equals(((TestValueSource) o).source);
    }

    @Override
    public int hashCode() {
      return source.hashCode() + TestValueSource.class.hashCode();
    }

    @Override
    public String description() {
      return "testfunc(" + source.description() + ')';
    }

    @Override
    public void createWeight(Map<Object, Object> context, IndexSearcher searcher)
        throws IOException {
      context.put(this, this);
    }

    @Override
    public SortField getSortField(boolean reverse) {
      return super.getSortField(reverse);
    }
  }
}

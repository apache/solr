package org.apache.solr.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queries.function.valuesource.DefFunction;
import org.apache.lucene.queries.function.valuesource.DivFloatFunction;
import org.apache.lucene.queries.function.valuesource.DocFreqValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.IDFValueSource;
import org.apache.lucene.queries.function.valuesource.IfFunction;
import org.apache.lucene.queries.function.valuesource.JoinDocFreqValueSource;
import org.apache.lucene.queries.function.valuesource.LinearFloatFunction;
import org.apache.lucene.queries.function.valuesource.LiteralValueSource;
import org.apache.lucene.queries.function.valuesource.MaxDocValueSource;
import org.apache.lucene.queries.function.valuesource.MaxFloatFunction;
import org.apache.lucene.queries.function.valuesource.MinFloatFunction;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.queries.function.valuesource.NormValueSource;
import org.apache.lucene.queries.function.valuesource.NumDocsValueSource;
import org.apache.lucene.queries.function.valuesource.ProductFloatFunction;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.queries.function.valuesource.RangeMapFloatFunction;
import org.apache.lucene.queries.function.valuesource.ReciprocalFloatFunction;
import org.apache.lucene.queries.function.valuesource.ScaleFloatFunction;
import org.apache.lucene.queries.function.valuesource.SumFloatFunction;
import org.apache.lucene.queries.function.valuesource.SumTotalTermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TFValueSource;
import org.apache.lucene.queries.function.valuesource.TermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TotalTermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.queries.payloads.PayloadDecoder;
import org.apache.lucene.queries.payloads.PayloadFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.CurrencyFieldType;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.search.facet.AggValueSource;
import org.apache.solr.search.facet.AvgAgg;
import org.apache.solr.search.facet.CountAgg;
import org.apache.solr.search.facet.CountValsAgg;
import org.apache.solr.search.facet.HLLAgg;
import org.apache.solr.search.facet.MinMaxAgg;
import org.apache.solr.search.facet.MissingAgg;
import org.apache.solr.search.facet.PercentileAgg;
import org.apache.solr.search.facet.RelatednessAgg;
import org.apache.solr.search.facet.StddevAgg;
import org.apache.solr.search.facet.SumAgg;
import org.apache.solr.search.facet.SumsqAgg;
import org.apache.solr.search.facet.UniqueAgg;
import org.apache.solr.search.facet.UniqueBlockFieldAgg;
import org.apache.solr.search.facet.UniqueBlockQueryAgg;
import org.apache.solr.search.facet.VarianceAgg;
import org.apache.solr.search.function.CollapseScoreFunction;
import org.apache.solr.search.function.ConcatStringFunction;
import org.apache.solr.search.function.EqualFunction;
import org.apache.solr.search.function.OrdFieldSource;
import org.apache.solr.search.function.ReverseOrdFieldSource;
import org.apache.solr.search.function.SolrComparisonBoolFunction;
import org.apache.solr.search.function.distance.GeoDistValueSourceParser;
import org.apache.solr.search.function.distance.GeohashFunction;
import org.apache.solr.search.function.distance.GeohashHaversineFunction;
import org.apache.solr.search.function.distance.HaversineFunction;
import org.apache.solr.search.function.distance.SquaredEuclideanFunction;
import org.apache.solr.search.function.distance.StringDistanceFunction;
import org.apache.solr.search.function.distance.VectorDistanceFunction;
import org.apache.solr.search.join.ChildFieldValueSourceParser;
import org.apache.solr.util.PayloadUtils;
import org.locationtech.spatial4j.distance.DistanceUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StdValueSourceParsers {
  /** standard functions supported by default, filled in static class initialization */
  static final Map<String, ValueSourceParser> standardVSParsers = new HashMap<>();
  /** standard functions supported by default */
  public static final Map<String, ValueSourceParser> standardValueSourceParsers
    = Collections.unmodifiableMap(standardVSParsers);

  /** Adds a new parser for the name and returns any existing one that was overridden.
   *  This is not thread safe.
   */
  static ValueSourceParser addParser(String name, ValueSourceParser p) {
    return standardVSParsers.put(name, p);
  }

  /** Adds a new parser for the name and returns any existing one that was overridden.
   *  This is not thread safe.
   */
  static ValueSourceParser addParser(ValueSourceParser.NamedParser p) {
    return standardVSParsers.put(p.name(), p);
  }

  private static void alias(String source, String dest) {
    StdValueSourceParsers.standardVSParsers.put(dest, StdValueSourceParsers.standardVSParsers.get(source));
  }

  private static TInfo parseTerm(FunctionQParser fp) throws SyntaxError {
    TInfo tinfo = new TInfo();

    tinfo.indexedField = tinfo.field = fp.parseArg();
    tinfo.val = fp.parseArg();
    tinfo.indexedBytes = new BytesRefBuilder();

    FieldType ft = fp.getReq().getSchema().getFieldTypeNoEx(tinfo.field);
    if (ft == null) ft = new StrField();

    if (ft instanceof TextField) {
      // need to do analysis on the term
      String indexedVal = tinfo.val;
      Query q = ft.getFieldQuery(fp, fp.getReq().getSchema().getFieldOrNull(tinfo.field), tinfo.val);
      if (q instanceof TermQuery) {
        Term term = ((TermQuery)q).getTerm();
        tinfo.indexedField = term.field();
        indexedVal = term.text();
      }
      tinfo.indexedBytes.copyChars(indexedVal);
    } else {
      ft.readableToIndexed(tinfo.val, tinfo.indexedBytes);
    }

    return tinfo;
  }

  private static void splitSources(int dim, List<ValueSource> sources, List<ValueSource> dest1, List<ValueSource> dest2) {
    //Get dim value sources for the first vector
    for (int i = 0; i < dim; i++) {
      dest1.add(sources.get(i));
    }
    //Get dim value sources for the second vector
    for (int i = dim; i < sources.size(); i++) {
      dest2.add(sources.get(i));
    }
  }

  private static class MVResult {
    MultiValueSource mv1;
    MultiValueSource mv2;
  }

  private static class TInfo {
    String field;
    String val;
    String indexedField;
    BytesRefBuilder indexedBytes;
  }

  private static MVResult getMultiValueSources(List<ValueSource> sources) {
    MVResult mvr = new MVResult();
    if (sources.size() % 2 != 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Illegal number of sources.  There must be an even number of sources");
    }
    if (sources.size() == 2) {

      //check to see if these are MultiValueSource
      boolean s1MV = sources.get(0) instanceof MultiValueSource;
      boolean s2MV = sources.get(1) instanceof MultiValueSource;
      if (s1MV && s2MV) {
        mvr.mv1 = (MultiValueSource) sources.get(0);
        mvr.mv2 = (MultiValueSource) sources.get(1);
      } else if (s1MV ||
          s2MV) {
        //if one is a MultiValueSource, than the other one needs to be too.
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Illegal number of sources.  There must be an even number of sources");
      } else {
        mvr.mv1 = new VectorValueSource(Collections.singletonList(sources.get(0)));
        mvr.mv2 = new VectorValueSource(Collections.singletonList(sources.get(1)));
      }
    } else {
      int dim = sources.size() / 2;
      List<ValueSource> sources1 = new ArrayList<>(dim);
      List<ValueSource> sources2 = new ArrayList<>(dim);
      //Get dim value sources for the first vector
      splitSources(dim, sources, sources1, sources2);
      mvr.mv1 = new VectorValueSource(sources1);
      mvr.mv2 = new VectorValueSource(sources2);
    }

    return mvr;
  }

  static {
    StdValueSourceParsers.addParser("testfunc", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        final ValueSource source = fp.parseValueSource();
        return new TestValueSource(source);
      }
    });
    StdValueSourceParsers.addParser("ord", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseId();
        return new OrdFieldSource(field);
      }
    });
    StdValueSourceParsers.addParser("literal", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new LiteralValueSource(fp.parseArg());
      }
    });
    StdValueSourceParsers.addParser("threadid", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new LongConstValueSource(Thread.currentThread().getId());
      }
    });
    StdValueSourceParsers.addParser("sleep", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        int ms = fp.parseInt();
        ValueSource source = fp.parseValueSource();
        try {
          Thread.sleep(ms);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new RuntimeException(e);
        }
        return source;
      }
    });
    StdValueSourceParsers.addParser("rord", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseId();
        return new ReverseOrdFieldSource(field);
      }
    });
    StdValueSourceParsers.addParser("top", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        // top(vs) is now a no-op
        ValueSource source = fp.parseValueSource();
        return source;
      }
    });
    StdValueSourceParsers.addParser("linear", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        float slope = fp.parseFloat();
        float intercept = fp.parseFloat();
        return new LinearFloatFunction(source, slope, intercept);
      }
    });
    StdValueSourceParsers.addParser("recip", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        float m = fp.parseFloat();
        float a = fp.parseFloat();
        float b = fp.parseFloat();
        return new ReciprocalFloatFunction(source, m, a, b);
      }
    });
    StdValueSourceParsers.addParser("scale", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        float min = fp.parseFloat();
        float max = fp.parseFloat();
        return new ScaleFloatFunction(source, min, max);
      }
    });
    StdValueSourceParsers.addParser("div", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DivFloatFunction(a, b);
      }
    });
    StdValueSourceParsers.addParser("mod", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DualFloatFunction(a, b);
      }
    });
    StdValueSourceParsers.addParser("map", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        float min = fp.parseFloat();
        float max = fp.parseFloat();
        ValueSource target = fp.parseValueSource();
        ValueSource def = fp.hasMoreArguments() ? fp.parseValueSource() : null;
        return new RangeMapFloatFunction(source, min, max, target, def);
      }
    });

    StdValueSourceParsers.addParser("abs", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        return new SimpleFloatFunction(source);
      }
    });
    StdValueSourceParsers.addParser("cscore", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new CollapseScoreFunction();
      }
    });
    StdValueSourceParsers.addParser("sum", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new SumFloatFunction(sources.toArray(EMPTY_VALUE_SOURCES));
      }
    });
    alias("sum","add");

    StdValueSourceParsers.addParser("product", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new ProductFloatFunction(sources.toArray(EMPTY_VALUE_SOURCES));
      }
    });
    alias("product","mul");

    StdValueSourceParsers.addParser("sub", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new MyDualFloatFunction(a, b);
      }
    });
    StdValueSourceParsers.addParser("vector", new ValueSourceParser(){
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new VectorValueSource(fp.parseValueSourceList());
      }
    });
    StdValueSourceParsers.addParser("query", new ValueSourceParser() {
      // boost(query($q),rating)
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        Query q = fp.parseNestedQuery();
        float defVal = 0.0f;
        if (fp.hasMoreArguments()) {
          defVal = fp.parseFloat();
        }
        return new QueryValueSource(q, defVal);
      }
    });
    StdValueSourceParsers.addParser("boost", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        Query q = fp.parseNestedQuery();
        ValueSource vs = fp.parseValueSource();
        return new QueryValueSource(FunctionScoreQuery.boostByValue(q, vs.asDoubleValuesSource()), 0.0f);
      }
    });
    StdValueSourceParsers.addParser("joindf", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String f0 = fp.parseArg();
        String qf = fp.parseArg();
        return new JoinDocFreqValueSource( f0, qf );
      }
    });

    StdValueSourceParsers.addParser("geodist", new GeoDistValueSourceParser());

    StdValueSourceParsers.addParser("hsin", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        double radius = fp.parseDouble();
        //SOLR-2114, make the convert flag required, since the parser doesn't support much in the way of lookahead or the ability to convert a String into a ValueSource
        boolean convert = Boolean.parseBoolean(fp.parseArg());

        MultiValueSource pv1;
        MultiValueSource pv2;

        ValueSource one = fp.parseValueSource();
        ValueSource two = fp.parseValueSource();
        if (fp.hasMoreArguments()) {
          pv1 = new VectorValueSource(Arrays.asList(one, two));//x1, y1
          pv2 = new VectorValueSource(Arrays.asList(fp.parseValueSource(), fp.parseValueSource()));//x2, y2
        } else {
          //check to see if we have multiValue source
          if (one instanceof MultiValueSource && two instanceof MultiValueSource){
            pv1 = (MultiValueSource) one;
            pv2 = (MultiValueSource) two;
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Input must either be 2 MultiValueSources, or there must be 4 ValueSources");
          }
        }

        return new HaversineFunction(pv1, pv2, radius, convert);
      }
    });

    StdValueSourceParsers.addParser("ghhsin", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        double radius = fp.parseDouble();

        ValueSource gh1 = fp.parseValueSource();
        ValueSource gh2 = fp.parseValueSource();

        return new GeohashHaversineFunction(gh1, gh2, radius);
      }
    });

    StdValueSourceParsers.addParser("geohash", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        ValueSource lat = fp.parseValueSource();
        ValueSource lon = fp.parseValueSource();

        return new GeohashFunction(lat, lon);
      }
    });
    StdValueSourceParsers.addParser("strdist", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        ValueSource str1 = fp.parseValueSource();
        ValueSource str2 = fp.parseValueSource();
        String distClass = fp.parseArg();

        StringDistance dist = null;
        if (distClass.equalsIgnoreCase("jw")) {
          dist = new JaroWinklerDistance();
        } else if (distClass.equalsIgnoreCase("edit")) {
          dist = new LevenshteinDistance();
        } else if (distClass.equalsIgnoreCase("ngram")) {
          int ngram = 2;
          if (fp.hasMoreArguments()) {
            ngram = fp.parseInt();
          }
          dist = new NGramDistance(ngram);
        } else {
          dist = fp.req.getCore().getResourceLoader().newInstance(distClass, StringDistance.class);
        }
        return new StringDistanceFunction(str1, str2, dist);
      }
    });
    StdValueSourceParsers.addParser("field", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        String fieldName = fp.parseArg();
        SchemaField f = fp.getReq().getSchema().getField(fieldName);
        if (fp.hasMoreArguments()) {
          // multivalued selector option
          String s = fp.parseArg();
          FieldType.MultiValueSelector selector = FieldType.MultiValueSelector.lookup(s);
          if (null == selector) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Multi-Valued field selector '"+s+"' not supported");
          }
          return f.getType().getSingleValueSource(selector, f, fp);
        }
        // simple field ValueSource
        return f.getType().getValueSource(f, fp);
      }
    });
    StdValueSourceParsers.addParser("currency", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        String fieldName = fp.parseArg();
        SchemaField f = fp.getReq().getSchema().getField(fieldName);
        if (! (f.getType() instanceof CurrencyFieldType)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Currency function input must be the name of a CurrencyFieldType: " + fieldName);
        }
        CurrencyFieldType ft = (CurrencyFieldType) f.getType();
        String code = fp.hasMoreArguments() ? fp.parseArg() : null;
        return ft.getConvertedValueSource(code, ft.getValueSource(f, fp));
      }
    });

    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("rad") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return vals.doubleVal(doc) * DistanceUtils.DEGREES_TO_RADIANS;
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("deg") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return vals.doubleVal(doc) * DistanceUtils.RADIANS_TO_DEGREES;
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("sqrt") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.sqrt(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("cbrt") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.cbrt(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("log") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.log10(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("ln") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.log(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("exp") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.exp(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("sin") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.sin(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("cos") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.cos(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("tan") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.tan(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("asin") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.asin(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("acos") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.acos(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("atan") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.atan(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("sinh") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.sinh(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("cosh") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.cosh(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("tanh") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.tanh(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("ceil") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.ceil(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("floor") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.floor(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.DoubleParser("rint") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.rint(vals.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.Double2Parser("pow") {
      @Override
      public double func(int doc, FunctionValues a, FunctionValues b) throws IOException {
        return Math.pow(a.doubleVal(doc), b.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.Double2Parser("hypot") {
      @Override
      public double func(int doc, FunctionValues a, FunctionValues b) throws IOException {
        return Math.hypot(a.doubleVal(doc), b.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser(new ValueSourceParser.Double2Parser("atan2") {
      @Override
      public double func(int doc, FunctionValues a, FunctionValues b) throws IOException {
        return Math.atan2(a.doubleVal(doc), b.doubleVal(doc));
      }
    });
    StdValueSourceParsers.addParser("max", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MaxFloatFunction(sources.toArray(EMPTY_VALUE_SOURCES));
      }
    });
    StdValueSourceParsers.addParser("min", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MinFloatFunction(sources.toArray(EMPTY_VALUE_SOURCES));
      }
    });

    StdValueSourceParsers.addParser("sqedist", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        MVResult mvr = getMultiValueSources(sources);

        return new SquaredEuclideanFunction(mvr.mv1, mvr.mv2);
      }
    });

    StdValueSourceParsers.addParser("dist", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        float power = fp.parseFloat();
        List<ValueSource> sources = fp.parseValueSourceList();
        MVResult mvr = getMultiValueSources(sources);
        return new VectorDistanceFunction(power, mvr.mv1, mvr.mv2);
      }
    });
    StdValueSourceParsers.addParser("ms", new ValueSourceParser.DateValueSourceParser());


    StdValueSourceParsers.addParser("pi", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new DoubleConstValueSource(Math.PI);
      }
    });
    StdValueSourceParsers.addParser("e", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new DoubleConstValueSource(Math.E);
      }
    });


    StdValueSourceParsers.addParser("docfreq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new DocFreqValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });

    StdValueSourceParsers.addParser("totaltermfreq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new TotalTermFreqValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });
    alias("totaltermfreq","ttf");

    StdValueSourceParsers.addParser("sumtotaltermfreq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseArg();
        return new SumTotalTermFreqValueSource(field);
      }
    });
    alias("sumtotaltermfreq","sttf");

    StdValueSourceParsers.addParser("idf", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new IDFValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });

    StdValueSourceParsers.addParser("termfreq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new TermFreqValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });

    StdValueSourceParsers.addParser("tf", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new TFValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });

    StdValueSourceParsers.addParser("norm", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseArg();
        return new NormValueSource(field);
      }
    });

    StdValueSourceParsers.addParser("maxdoc", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new MaxDocValueSource();
      }
    });

    StdValueSourceParsers.addParser("numdocs", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new NumDocsValueSource();
      }
    });

    StdValueSourceParsers.addParser("payload", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        // payload(field,value[,default, ['min|max|average|first']])
        //   defaults to "average" and 0.0 default value

        TInfo tinfo = parseTerm(fp); // would have made this parser a new separate class and registered it, but this handy method is private :/

        ValueSource defaultValueSource;
        if (fp.hasMoreArguments()) {
          defaultValueSource = fp.parseValueSource();
        } else {
          defaultValueSource = new ConstValueSource(0.0f);
        }

        PayloadFunction payloadFunction;
        String func = "average";
        if (fp.hasMoreArguments()) {
          func = fp.parseArg();
        }
        payloadFunction = PayloadUtils.getPayloadFunction(func);

        // Support func="first" by payloadFunction=null
        if(payloadFunction == null && !"first".equals(func)) {
          // not "first" (or average, min, or max)
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid payload function: " + func);
        }

        IndexSchema schema = fp.getReq().getCore().getLatestSchema();
        PayloadDecoder decoder = schema.getPayloadDecoder(tinfo.field);

        if (decoder==null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No payload decoder found for field: " + tinfo.field);
        }

        return new FloatPayloadValueSource(
            tinfo.field,
            tinfo.val,
            tinfo.indexedField,
            tinfo.indexedBytes.get(),
            decoder,
            payloadFunction,
            defaultValueSource);
      }
    });

    StdValueSourceParsers.addParser("true", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return BoolConstValueSource.TRUE;
      }
    });

    StdValueSourceParsers.addParser("false", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return BoolConstValueSource.FALSE;
      }
    });

    StdValueSourceParsers.addParser("exists", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource vs = fp.parseValueSource();
        return new SimpleBoolFunction(vs);
      }
    });

    StdValueSourceParsers.addParser("not", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource vs = fp.parseValueSource();
        return new SimpleBoolFunction2(vs);
      }
    });


    StdValueSourceParsers.addParser("and", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MultiBoolFunction(sources);
      }
    });

    StdValueSourceParsers.addParser("or", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MultiBoolFunction2(sources);
      }
    });

    StdValueSourceParsers.addParser("xor", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MultiBoolFunction3(sources);
      }
    });

    StdValueSourceParsers.addParser("if", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource ifValueSource = fp.parseValueSource();
        ValueSource trueValueSource = fp.parseValueSource();
        ValueSource falseValueSource = fp.parseValueSource();

        return new IfFunction(ifValueSource, trueValueSource, falseValueSource);
      }
    });

    StdValueSourceParsers.addParser("gt", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new SolrComparisonBoolFunction(lhsValSource, rhsValSource, "gt", (cmp) -> cmp > 0);
      }
    });

    StdValueSourceParsers.addParser("lt", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new SolrComparisonBoolFunction(lhsValSource, rhsValSource, "lt", (cmp) -> cmp < 0);
      }
    });

    StdValueSourceParsers.addParser("gte", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new SolrComparisonBoolFunction(lhsValSource, rhsValSource, "gte", (cmp) -> cmp >= 0);

      }
    });

    StdValueSourceParsers.addParser("lte", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new SolrComparisonBoolFunction(lhsValSource, rhsValSource, "lte", (cmp) -> cmp <= 0);
      }
    });

    StdValueSourceParsers.addParser("eq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new EqualFunction(lhsValSource, rhsValSource, "eq");
      }
    });

    StdValueSourceParsers.addParser("def", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new DefFunction(fp.parseValueSourceList());
      }
    });

    StdValueSourceParsers.addParser("concat", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new ConcatStringFunction(sources.toArray(EMPTY_VALUE_SOURCES));
      }
    });


    StdValueSourceParsers.addParser("agg", new ValueSourceParser() {
      @Override
      public AggValueSource parse(FunctionQParser fp) throws SyntaxError {
        return fp.parseAgg(FunctionQParser.FLAG_DEFAULT);
      }
    });

    StdValueSourceParsers.addParser("agg_count", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new CountAgg();
      }
    });

    StdValueSourceParsers.addParser("agg_unique", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new UniqueAgg(fp.parseArg());
      }
    });

    StdValueSourceParsers.addParser("agg_uniqueBlock", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        if (fp.sp.peek() == QueryParsing.LOCALPARAM_START.charAt(0) ) {
          return new UniqueBlockQueryAgg(fp.parseNestedQuery());
        }
        return new UniqueBlockFieldAgg(fp.parseArg());
      }
    });

    StdValueSourceParsers.addParser("agg_hll", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new HLLAgg(fp.parseArg());
      }
    });

    StdValueSourceParsers.addParser("agg_sum", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new SumAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    StdValueSourceParsers.addParser("agg_avg", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new AvgAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    StdValueSourceParsers.addParser("agg_sumsq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new SumsqAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    StdValueSourceParsers.addParser("agg_variance", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new VarianceAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    StdValueSourceParsers.addParser("agg_stddev", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new StddevAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    StdValueSourceParsers.addParser("agg_missing", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new MissingAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    StdValueSourceParsers.addParser("agg_countvals", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new CountValsAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    /***
     addParser("agg_multistat", new ValueSourceParser() {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    return null;
    }
    });
     ***/

    StdValueSourceParsers.addParser("agg_min", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new MinMaxAgg("min", fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    StdValueSourceParsers.addParser("agg_max", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new MinMaxAgg("max", fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    StdValueSourceParsers.addParser("agg_percentile", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<Double> percentiles = new ArrayList<>();
        ValueSource vs = fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE);
        while (fp.hasMoreArguments()) {
          double val = fp.parseDouble();
          if (val<0 || val>100) {
            throw new SyntaxError("requested percentile must be between 0 and 100.  got " + val);
          }
          percentiles.add(val);
        }

        if (percentiles.isEmpty()) {
          throw new SyntaxError("expected percentile(valsource,percent1[,percent2]*)  EXAMPLE:percentile(myfield,50)");
        }

        return new PercentileAgg(vs, percentiles);
      }
    });

    StdValueSourceParsers.addParser("agg_" + RelatednessAgg.NAME, new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        // TODO: (fore & back)-ground should be optional -- use hasMoreArguments
        // if only one arg, assume it's the foreground
        // (background is the one that will most commonly just be "*:*")
        // see notes in RelatednessAgg constructor about why we don't do this yet
        RelatednessAgg agg = new RelatednessAgg(fp.parseNestedQuery(), fp.parseNestedQuery());
        agg.setOpts(fp);
        return agg;
      }
    });

    StdValueSourceParsers.addParser("childfield", new ChildFieldValueSourceParser());
  }

  ///////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////
}

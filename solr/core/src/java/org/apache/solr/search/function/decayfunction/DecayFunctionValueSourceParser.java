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

package org.apache.solr.search.function.decayfunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.*;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.util.DateMathParser;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class DecayFunctionValueSourceParser extends ValueSourceParser {
  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {

    String field = fp.parseArg();
    SchemaField f = fp.getReq().getSchema().getField(field);
    FieldType ft = f.getType();

    // if DatePointField field then assume dates -> numericvaluesource
    if (ft instanceof DatePointField) {
      return parseDateVariable(fp, ft.getValueSource(f, fp));
    }
    // if LatLonPointSpatialField then assume distances -> geovaluesource
    else if (ft instanceof LatLonPointSpatialField) {
      return parseGeoVariable(fp, field, (LatLonPointSpatialField) ft);
    }
    // if NumericFieldType field -> numericvaluesource
    else if (ft instanceof NumericFieldType) {
      return parseNumericVariable(fp, ft.getValueSource(f, fp));
    }
    // else exception
    else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          String.format(
              "field [%s] is of type [%s], but only numeric types are supported.", field, ft));
    }
  }

  abstract DecayStrategy getDecayStrategy();

  abstract String name();

  protected ValueSource parseGeoVariable(
      FunctionQParser fp, String field, LatLonPointSpatialField ft) throws SyntaxError {

    String scaleString = fp.parseArg();
    if (scaleString == null)
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "[scale] must be set for geo fields.");

    double lat = fp.parseDouble();
    double lon = fp.parseDouble();
    SpatialStrategy strategy = ft.getStrategy(field);
    Point origin = strategy.getSpatialContext().getShapeFactory().pointXY(lon, lat);
    String offsetString = fp.hasMoreArguments() ? fp.parseArg() : "0km";
    double decay = fp.hasMoreArguments() ? fp.parseDouble() : 0.5;

    ValueSource vs =
        ValueSource.fromDoubleValuesSource(
            strategy.makeDistanceValueSource(
                origin, DistanceUtils.DEG_TO_KM)); // this does the distance calculation from origin

    double scale = DistanceUnit.KILOMETERS.parse(scaleString, DistanceUnit.KILOMETERS);
    double offset = DistanceUnit.KILOMETERS.parse(offsetString, DistanceUnit.KILOMETERS);

    return new GeoDecayFunctionValueSource(scale, offset, decay, getDecayStrategy(), vs, name());
  }

  protected ValueSource parseNumericVariable(FunctionQParser fp, ValueSource vs)
      throws SyntaxError {
    double scale = fp.parseDouble();
    double origin = fp.parseDouble();
    double offset = fp.hasMoreArguments() ? fp.parseDouble() : 0;
    double decay = fp.hasMoreArguments() ? fp.parseDouble() : 0.5; // def 0.5
    return new NumericDecayFunctionValueSource(
        origin, scale, decay, offset, getDecayStrategy(), vs, name());
  }

  protected ValueSource parseDateVariable(FunctionQParser fp, ValueSource vs) throws SyntaxError {
    String originString;
    String offsetString = "+0DAY";
    double decay = 0.5;

    String scaleString = fp.parseArg(); // 1st arg
    if (scaleString == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "[scale] must be set for date fields.");
    }

    originString = fp.parseArg(); // 2nd arg
    Date originDate =
        DateMathParser.parseMath(null, Optional.ofNullable(originString).orElse("NOW"));
    long origin = originDate.getTime();

    long scale = DateMathParser.parseMath(originDate, "NOW" + scaleString).getTime() - origin;

    if (fp.hasMoreArguments()) offsetString = fp.parseArg(); // 3rd arg
    long offset = DateMathParser.parseMath(originDate, "NOW" + offsetString).getTime() - origin;

    if (fp.hasMoreArguments()) decay = fp.parseDouble();

    return new NumericDecayFunctionValueSource(
        origin, scale, decay, offset, getDecayStrategy(), vs, name());
  }
}

class GeoDecayFunctionValueSource extends ValueSource {

  private final double scale;
  private final double offset;
  private final double decay;
  private final DecayStrategy decayStrategy;
  private final ValueSource vs;
  private final String name;

  public GeoDecayFunctionValueSource(
      double scale,
      double offset,
      double decay,
      DecayStrategy decayStrategy,
      ValueSource vs,
      String name) {
    this.scale = scale;
    this.offset = offset;
    this.decay = decay;
    this.decayStrategy = decayStrategy;
    this.vs = vs;
    this.name = name;
  }

  @Override
  public FunctionValues getValues(Map<Object,Object> context, LeafReaderContext readerContext) throws IOException {
    FunctionValues values = vs.getValues(context, readerContext);
    double s = decayStrategy.scale(scale, decay);
    return new DoubleDocValues(vs) {

      @Override
      public double doubleVal(int doc) throws IOException {
        double distance = values.doubleVal(doc);
        return decayStrategy.calculate(Math.max(0.0d, distance - offset), s);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GeoDecayFunctionValueSource that = (GeoDecayFunctionValueSource) o;
    return Objects.equals(scale, that.scale)
        && Objects.equals(offset, that.offset)
        && Objects.equals(decay, that.decay)
        && Objects.equals(decayStrategy, that.decayStrategy)
        && Objects.equals(vs, that.vs)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scale, offset, decay, decayStrategy, vs, name);
  }

  @Override
  public String description() {
    return name + "(" + decayStrategy.explain(scale) + ")";
  }
}

class NumericDecayFunctionValueSource extends ValueSource {

  private final double origin;
  private final double scale;
  private final double decay;
  private final double offset;
  private final DecayStrategy decayStrategy;
  private final ValueSource vs;
  private final String name;

  public NumericDecayFunctionValueSource(
      double origin,
      double scale,
      double decay,
      double offset,
      DecayStrategy decayStrategy,
      ValueSource vs,
      String name) {
    this.origin = origin;
    this.scale = scale;
    this.decay = decay;
    this.offset = offset;
    this.decayStrategy = decayStrategy;
    this.vs = vs;
    this.name = name;
  }

  @Override
  public FunctionValues getValues(Map<Object,Object> context, LeafReaderContext readerContext) throws IOException {
    double s = decayStrategy.scale(scale, decay);
    FunctionValues values = vs.getValues(context, readerContext);
    return new DoubleDocValues(vs) {
      @Override
      public double doubleVal(int doc) throws IOException {
        double val = values.doubleVal(doc);
        return decayStrategy.calculate(Math.max(0.0d, Math.abs(val - origin) - offset), s);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NumericDecayFunctionValueSource that = (NumericDecayFunctionValueSource) o;
    return Objects.equals(origin, that.origin)
        && Objects.equals(scale, that.scale)
        && Objects.equals(offset, that.offset)
        && Objects.equals(decayStrategy, that.decayStrategy)
        && Objects.equals(vs, that.vs)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(origin, scale, offset, decayStrategy, vs, name);
  }

  @Override
  public String description() {
    return name + "(" + decayStrategy.explain(scale) + ")";
  }
}

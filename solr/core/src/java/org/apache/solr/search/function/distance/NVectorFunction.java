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

package org.apache.solr.search.function.distance;

import static org.apache.solr.util.NVectorUtil.NVectorDist;
import static org.apache.solr.util.NVectorUtil.NVectorDotProduct;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.search.*;
import org.apache.solr.common.SolrException;

public class NVectorFunction extends ValueSource {
  private final MultiValueSource nvector1;
  private final MultiValueSource nvector2;
  private final double radius;

  public NVectorFunction(MultiValueSource nvector1, MultiValueSource nvector2, double radius) {
    this.nvector1 = nvector1;
    this.nvector2 = nvector2;
    if (nvector1.dimension() != 3 || nvector2.dimension() != 3) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Illegal dimension for value sources");
    }
    this.radius = radius;
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
          throws IOException {

    final FunctionValues fv = getDotProductValues(context,readerContext);

    return new DoubleDocValues(this) {

      @Override
      public double doubleVal(int doc) throws IOException {
        double dotProduct = fv.doubleVal(doc);
        return NVectorDist(dotProduct, radius);
      }

      @Override
      public String toString(int doc) throws IOException {
        return fv.toString(doc);
      }
    };
  }
  public FunctionValues getDotProductValues(Map<Object, Object> context, LeafReaderContext readerContext) throws IOException {

    final FunctionValues nvector_v1 = nvector1.getValues(context, readerContext);
    final FunctionValues nvector_v2 = nvector2.getValues(context, readerContext);
    return new DoubleDocValues(this) {

      @Override
      public double doubleVal(int doc) throws IOException {
        double[] nvector_dv1 = new double[nvector1.dimension()];
        double[] nvector_dv2 = new double[nvector2.dimension()];
        nvector_v1.doubleVal(doc, nvector_dv1);
        nvector_v2.doubleVal(doc, nvector_dv2);
        return NVectorDotProduct(nvector_dv1, nvector_dv2);
      }

      @Override
      public String toString(int doc) throws IOException {
        return name() + ',' + nvector_v1.toString(doc) + ',' + nvector_v2.toString(doc) + ')';
      }
    };
  }

  protected String name() {
    return "nvector";
  }
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NVectorFunction that = (NVectorFunction) o;
    return Double.compare(that.radius, radius) == 0
        && nvector1.equals(that.nvector1)
        && nvector2.equals(that.nvector2);
  }
  @Override
  public int hashCode() {
    return Objects.hash(nvector1, nvector2, radius);
  }
  @Override
  public void createWeight(Map<Object, Object> context, IndexSearcher searcher) throws IOException {
    nvector1.createWeight(context, searcher);
    nvector2.createWeight(context, searcher);
  }
  @Override
  public String description() {
    return name() + '(' + nvector1 + ',' + nvector2 + ')';
  }
  @Override
  public SortField getSortField(boolean reverse){ return new NVectorValueSourceSortField(reverse);  }
  class NVectorValueSourceSortField extends SortField {
    public NVectorValueSourceSortField(boolean reverse) {
      super(description(), SortField.Type.REWRITEABLE, reverse);
    }

    @Override
    public SortField rewrite(IndexSearcher searcher) throws IOException {
      Map<Object, Object> context = ValueSource.newContext(searcher);
      createWeight(context, searcher);
      return new SortField(getField(), new NVectorValueSourceComparatorSource(context), getReverse());
    }
  }
  class NVectorValueSourceComparatorSource extends FieldComparatorSource {
    private final Map<Object, Object> context;

    public NVectorValueSourceComparatorSource(Map<Object, Object> context) {
      this.context = context;
    }

    @Override
    public FieldComparator<Double> newComparator(
            String fieldname, int numHits, boolean enableSkipping, boolean reversed) {
      return new NVectorValueSourceComparator(context, numHits);
    }
  }
  //Please note: The comparisons are INVERTED here, for performance on sorting we compare
  // with the dot product, which is negatively correlated to acos(dot_product). Converting the dot product to distance with d=R*acos(dot_product), for example:
  // where d = distance, dp = dot_product
  // d:19628.29698448594 dp:-0.9981573955675561
  //d:18583.651644725564 dp:-0.9748645959351536
  //d:18180.44788490305 dp:-0.9588220684898193
  //d:18052.6639699517 dp:-0.9529332319157707
  //d:17882.7920545206 dp:-0.9445116994940388
  //....
  //d:1930.2292960794525 dp:0.954454358625817
  //d:1843.0056018566079 dp:0.9584495044607074
  //d:455.63526860635756 dp:0.9974437510266485
  //d:336.9277320011129 dp:0.9986019397287412
  //d:0.0 dp:1.0
  // A HIGHER dot_product equates to a closer (LOWER) distance hence we invert
  class NVectorValueSourceComparator extends SimpleFieldComparator<Double> {
    private final double[] values;
    private FunctionValues docVals;
    private double bottom;
    private final Map<Object, Object> fcontext;
    private double topValue;

    NVectorValueSourceComparator(Map<Object, Object> fcontext, int numHits) {
      this.fcontext = fcontext;
      values = new double[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Double.compare(values[slot2],values[slot1]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Double.compare(docVals.doubleVal(doc),bottom);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values[slot] = docVals.doubleVal(doc);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      docVals = getDotProductValues(fcontext, context);
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(final Double value) {
      this.topValue = value;
    }

    @Override
    public Double value(int slot) {
      return values[slot];
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return Double.compare(docVals.doubleVal(doc),topValue);
    }
  }
}



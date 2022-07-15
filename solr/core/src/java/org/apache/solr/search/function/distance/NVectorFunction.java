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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.search.IndexSearcher;
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

    final FunctionValues nvector_v1 = nvector1.getValues(context, readerContext);
    final FunctionValues nvector_v2 = nvector2.getValues(context, readerContext);

    return new DoubleDocValues(this) {

      @Override
      public double doubleVal(int doc) throws IOException {
        double[] nvector_dv1 = new double[nvector1.dimension()];
        double[] nvector_dv2 = new double[nvector2.dimension()];
        nvector_v1.doubleVal(doc, nvector_dv1);
        nvector_v2.doubleVal(doc, nvector_dv2);
        return NVectorDist(nvector_dv1, nvector_dv2, radius);
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
}

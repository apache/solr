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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.util.Map;

import static org.apache.solr.util.NVectorUtil.NVectorDist;

public class NVector extends ValueSource {

    private final MultiValueSource p1;
    private final MultiValueSource p2;
    private final double radius;

    public NVector(MultiValueSource p1, MultiValueSource p2, double radius) {
        this.p1 = p1;
        this.p2 = p2;
        if (p1.dimension() != 3 || p2.dimension() != 3) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Illegal dimension for value sources");
        }
        this.radius = radius;
    }

    @Override
    public FunctionValues getValues(Map<Object,Object> context, LeafReaderContext readerContext) throws IOException {

        final FunctionValues vals1 = p1.getValues(context, readerContext);
        final FunctionValues vals2 = p2.getValues(context, readerContext);

        return new DoubleDocValues(this) {

            @Override
            public double doubleVal(int doc) throws IOException {
                double[] dv1 = new double[p1.dimension()];
                double[] dv2 = new double[p2.dimension()];
                vals1.doubleVal(doc, dv1);
                vals2.doubleVal(doc, dv2);
                return  NVectorDist(dv1, dv2, radius);
            }

            @Override
            public String toString(int doc) throws IOException {
                return name() +
                    ',' +
                    vals1.toString(doc) +
                    ',' +
                    vals2.toString(doc) +
                    ')';
            }
        };
    }

    protected String name() {
        return "nvector";
    }

    @Override
    public boolean equals(Object o) {
        if (this.getClass() != o.getClass()) return false;
        NVector other = (NVector) o;
        return this.name()
            .equals(other.name())
            && p1.equals(other.p1) &&
            p2.equals(other.p2) && radius == other.radius;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = p1.hashCode();
        result = 31 * result + p2.hashCode();
        result = 31 * result + name().hashCode();
        temp = Double.doubleToRawLongBits(radius);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public void createWeight(Map<Object,Object> context, IndexSearcher searcher) throws IOException {
        p1.createWeight(context, searcher);
        p2.createWeight(context, searcher);
    }

    @Override
    public String description() {
        return name() + '(' +
            p1 + ',' + p2 +
            ')';
    }
}

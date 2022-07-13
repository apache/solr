
//Copyright (c) 2021, Dan Rosher
//    All rights reserved.
//
//    This source code is licensed under the BSD-style license found in the
//    LICENSE file in the root directory of this source tree.

package org.apache.solr.search.function.distance;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.util.NVectorUtil;

import java.util.Arrays;

public class NVectorValueSourceParser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        double lat = fp.parseDouble();
        double lon = fp.parseDouble();

        ValueSource vs1 = fp.parseValueSource();
        if (!(vs1 instanceof MultiValueSource))
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Field must a MultiValueSource");
        MultiValueSource mvs1 = (MultiValueSource) vs1;

        double[] nvector = NVectorUtil.latLongToNVector(lat, lon);
        MultiValueSource mvs2 = new VectorValueSource(
            Arrays.asList(
                new DoubleConstValueSource(nvector[0]),
                new DoubleConstValueSource(nvector[1]),
                new DoubleConstValueSource(nvector[2])
            ));

        double radius = fp.hasMoreArguments() ? fp.parseDouble() : NVectorUtil.EARTH_RADIUS;

        return new NVector(mvs1, mvs2, radius);
    }
}

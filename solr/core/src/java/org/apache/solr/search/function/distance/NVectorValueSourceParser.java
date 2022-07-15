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

import static org.locationtech.spatial4j.distance.DistanceUtils.EARTH_MEAN_RADIUS_KM;

import java.util.Arrays;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.util.NVectorUtil;

public class NVectorValueSourceParser extends ValueSourceParser {
  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    double lat = fp.parseDouble();
    double lon = fp.parseDouble();

    ValueSource vs1 = fp.parseValueSource();
    if (!(vs1 instanceof MultiValueSource))
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field must a MultiValueSource");
    MultiValueSource nvector_vs1 = (MultiValueSource) vs1;

    double[] nvector = NVectorUtil.latLongToNVector(lat, lon);

    MultiValueSource nvector_vs2 =
        new VectorValueSource(
            Arrays.asList(
                new DoubleConstValueSource(nvector[0]),
                new DoubleConstValueSource(nvector[1]),
                new DoubleConstValueSource(nvector[2])));

    double radius = fp.hasMoreArguments() ? fp.parseDouble() : EARTH_MEAN_RADIUS_KM;

    return new NVectorFunction(nvector_vs1, nvector_vs2, radius);
  }
}

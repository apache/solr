
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

package org.apache.solr.util;

import org.junit.Test;

import java.text.DecimalFormat;

import static org.junit.Assert.assertEquals;

public class NVectorUtilTest {

    DecimalFormat df = new DecimalFormat("##.####");

    @Test
    public void latLongToNVector() {
        double lat = 52.024535;
        double lon = -0.490155;
        double[] n = NVectorUtil.latLongToNVector(lat, lon);
        double[] ll = NVectorUtil.NVectorToLatLong(n);
        assertSimilar(lat, ll[0]);
        assertSimilar(lon, ll[1]);
    }

    void assertSimilar(double expected, double actual) {
        assertEquals(df.format(expected), df.format(actual));
    }

    @Test
    public void latLongToNVectorStr() {
        String lat = "52.024535";
        String lon = "-0.490155";
        String[] n = NVectorUtil.latLongToNVector(lat, lon);
        double[] ll = NVectorUtil.NVectorToLatLong(n);
        assertSimilar(Double.parseDouble(lat), ll[0]);
        assertSimilar(Double.parseDouble(lon), ll[1]);
    }

    @Test
    public void NVectorDist() {
        double[] a = NVectorUtil.latLongToNVector(52.019819, -0.490155);
        double[] b = NVectorUtil.latLongToNVector(52.019660, -0.498308);
        double dist = NVectorUtil.NVectorDist(a, b);
        assertEquals(0.5408290558849004, dist,0);
        a = NVectorUtil.latLongToNVector(52.02456414691066, -0.49013542948214134);
        b = NVectorUtil.latLongToNVector(51.92756819110318, -0.18695373636718815);
        assertEquals(22.673000657942616, NVectorUtil.NVectorDist(a, b),0);
    }
}
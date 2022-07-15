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

import org.apache.solr.SolrTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.locationtech.spatial4j.distance.DistanceUtils.EARTH_MEAN_RADIUS_KM;

public class FastInvTrigTest extends SolrTestCase {

    final static int num_points = 100000;
    final static double EPSILON = 0.0001;
    //static final Random r = new Random();
    private static final double TEN_METERS = 0.01;

    static double[][] points = new double[num_points][2];

    @Before
    public void initAll() {
        for (int i = 0; i < num_points; i++) {
            points[i] = generateRandomPoint();
        }
    }

    public static double deg2rad(double deg) {
        return deg * (Math.PI / 180);
    }

    public static double[] generateRandomPoint() {
        double u = random().nextDouble();
        double v = random().nextDouble();

        double latitude = deg2rad(Math.toDegrees(Math.acos(u * 2 - 1)) - 90);
        double longitude = deg2rad(360 * v - 180);
        return new double[]{latitude, longitude};
    }

    @Test
    public void acos() {
        for (double i = -1; i <= 1; i = i + 0.00001) {
            assertTrue(FastInvTrig.acos(i) - Math.acos(i) <= EPSILON);
        }
    }

    @Test
    public void dist() {
        double[] a = NVectorUtil.latLongToNVector(52.02456414691066, -0.49013542948214134);

        for (int i = 0; i < num_points; i++) {
            double[] b = NVectorUtil.latLongToNVector(points[i][0], points[i][1]);
            double d1 = EARTH_MEAN_RADIUS_KM * FastInvTrig.acos(a[0] * b[0] + a[1] * b[1] + a[2] * b[2]);
            double d2 = EARTH_MEAN_RADIUS_KM * Math.acos(a[0] * b[0] + a[1] * b[1] + a[2] * b[2]);
            assertEquals("Math.acos should be close to FastInvTrig.acos",d1,d2 , TEN_METERS);
        }
    }
}
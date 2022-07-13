
//Copyright (c) 2021, Dan Rosher
//    All rights reserved.
//
//    This source code is licensed under the BSD-style license found in the
//    LICENSE file in the root directory of this source tree.

package org.apache.solr.util;

import org.apache.solr.SolrTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.apache.solr.util.NVectorUtil.EARTH_RADIUS;

public class FastInvTrigTest extends SolrTestCase {

    final static int num_points = 100000;
    final static double EPSILON = 0.0001;
    static final Random r = new Random();
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
        double u = r.nextDouble();
        double v = r.nextDouble();

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
            double d1 = EARTH_RADIUS * FastInvTrig.acos(a[0] * b[0] + a[1] * b[1] + a[2] * b[2]);
            double d2 = EARTH_RADIUS * Math.acos(a[0] * b[0] + a[1] * b[1] + a[2] * b[2]);
            assertTrue(Math.abs(d1 - d2) <= TEN_METERS);
        }
    }
}
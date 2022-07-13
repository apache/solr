
//Copyright (c) 2021, Dan Rosher
//    All rights reserved.
//
//    This source code is licensed under the BSD-style license found in the
//    LICENSE file in the root directory of this source tree.

package org.apache.solr.util;

public class NVectorUtil {

    public static final double EARTH_RADIUS = 6173.008;//km google:standard mean earth radius;

    public static double[] latLongToNVector(double lat, double lon) {
        double latRad = lat * (Math.PI / 180);
        double lonRad = lon * (Math.PI / 180);
        double x = Math.cos(latRad) * Math.cos(lonRad);
        double y = Math.cos(latRad) * Math.sin(lonRad);
        double z = Math.sin(latRad);
        return new double[]{x, y, z};
    }

    public static String[] latLongToNVector(String lat, String lon) {
        double[] nvec = latLongToNVector(Double.parseDouble(lat), Double.parseDouble(lon));
        return new String[]{Double.toString(nvec[0]), Double.toString(nvec[1]), Double.toString(nvec[2])};
    }

    public static String[] latLongToNVector(String[] latlon) {
        return latLongToNVector(latlon[0], latlon[1]);
    }

    public static double[] NVectorToLatLong(double[] n) {
        return new double[]{Math.asin(n[2]) * (180 / Math.PI), Math.atan(n[1] / n[0]) * (180 / Math.PI)};
    }

    public static double[] NVectorToLatLong(String[] n) {
        return NVectorToLatLong(new double[]{
            Double.parseDouble(n[0]),
            Double.parseDouble(n[1]),
            Double.parseDouble(n[2])});
    }

    public static double NVectorDist(double[] a, double[] b) {
        return NVectorDist(a, b, EARTH_RADIUS);
    }

    public static double NVectorDist(double[] a, double[] b, double radius) {
        return radius * FastInvTrig.acos(a[0] * b[0] + a[1] * b[1] + a[2] * b[2]);
    }
}

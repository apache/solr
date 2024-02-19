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

import org.apache.lucene.util.SloppyMath;
import org.apache.solr.schema.NVectorField;
import org.apache.solr.search.function.distance.NVectorFunction;

import java.text.NumberFormat;
import java.text.ParseException;

import static org.locationtech.spatial4j.distance.DistanceUtils.EARTH_MEAN_RADIUS_KM;

/**
 *  NVectorUtil : This class contains helper methods used by  {@link NVectorFunction} and {@link NVectorField}
 *  to convert between n-vectors and lat,lon as well as calculating dot product for sorting and
 *  calculating the great circle (surface) distance
 */
public class NVectorUtil {

  private static final double pip2 = Math.PI/2;

  /**
   *
   * @param lat the latitude
   * @param lon the longitude
   * @return the NVector as double[3]
   */
  public static double[] latLongToNVector(double lat, double lon) {
    double latRad = Math.toRadians(lat);
    double lonRad = Math.toRadians(lon);
    double x = Math.cos(latRad) * Math.cos(lonRad);
    double y = Math.cos(latRad) * Math.sin(lonRad);
    double z = Math.sin(latRad);
    return new double[] {x, y, z};
  }


    /**
     * @param lat the latitude
     * @param lon the longitude
     * @return string rep of the n-vector
     */
  public static String[] latLongToNVector(String lat, String lon) {
    double[] nvec = latLongToNVector(Double.parseDouble(lat), Double.parseDouble(lon));
    return new String[] {
      Double.toString(nvec[0]), Double.toString(nvec[1]), Double.toString(nvec[2])
    };
  }

    /**
     * @param point  string rep of lat,lon
     * @param formatter  for parsing the string into a double wrt the locale
     * @return  string rep of the n-vector
     * @throws ParseException If the string for point cannot be parsed
     */
    public static String[] latLongToNVector(String[] point, NumberFormat formatter) throws ParseException {
    double[] nvec = latLongToNVector(formatter.parse(point[0]).doubleValue(),formatter.parse(point[1]).doubleValue());
    return new String[] {
            Double.toString(nvec[0]), Double.toString(nvec[1]), Double.toString(nvec[2])
    };
  }

    /**
     * @param n the nvector
     * @return the lat lon for this n-vector
     */
  public static double[] nVectorToLatLong(double[] n) {
    return new double[] {
            Math.toDegrees(Math.asin(n[2])),Math.toDegrees(Math.atan(n[1] / n[0]))
    };
  }

  public static double[] nVectorToLatLong(String[] n) {
    return nVectorToLatLong(
        new double[] {
          Double.parseDouble(n[0]), Double.parseDouble(n[1]), Double.parseDouble(n[2])
        });
  }

    /**
     * @param a the first n-vector
     * @param b the second n-vector
     * @return scalar doc product of both n-vectors
     */
  public static double nVectorDotProduct(double[] a, double[] b) {
    return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
  }

    /**
     * @param a the first n-vector
     * @param b the second n-vector
     * @return the great circle (surface) distance between the two n-vectors
     */
  public static double nVectorDist(double[] a, double[] b) {
    return nVectorDist(a, b, EARTH_MEAN_RADIUS_KM);
  }

    /**
     * @param a the first n-vector
     * @param b the second n-vector
     * @param radius he radius of the ellipsoid
     * @return the great circle (surface) distance between the two n-vectors
     */
  public static double nVectorDist(double[] a, double[] b, double radius) {
    return nVectorDist(nVectorDotProduct(a, b), radius);
  }

    /**
     * @param dotProduct the scalar dot product of two n-vectors
     * @param radius the radius of the ellipsoid
     * @return the great circle (surface) distance between the two n-vectors
     */
  public static double nVectorDist(double dotProduct, double radius){
    return radius * (pip2 - SloppyMath.asin(dotProduct));
  }

}

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

import java.text.NumberFormat;
import java.text.ParseException;

import static org.locationtech.spatial4j.distance.DistanceUtils.EARTH_MEAN_RADIUS_KM;

public class NVectorUtil {

  public static double[] latLongToNVector(double lat, double lon) {
    double latRad = lat * (Math.PI / 180);
    double lonRad = lon * (Math.PI / 180);
    double x = Math.cos(latRad) * Math.cos(lonRad);
    double y = Math.cos(latRad) * Math.sin(lonRad);
    double z = Math.sin(latRad);
    return new double[] {x, y, z};
  }

  public static String[] latLongToNVector(String lat, String lon) {
    double[] nvec = latLongToNVector(Double.parseDouble(lat), Double.parseDouble(lon));
    return new String[] {
      Double.toString(nvec[0]), Double.toString(nvec[1]), Double.toString(nvec[2])
    };
  }

  public static String[] latLongToNVector(String[] latlon) {
    return latLongToNVector(latlon[0], latlon[1]);
  }

  public static String[] latLongToNVector(String[] point, NumberFormat formatter) throws ParseException {
    double[] nvec = latLongToNVector(formatter.parse(point[0]).doubleValue(),formatter.parse(point[1]).doubleValue());
    return new String[] {
            Double.toString(nvec[0]), Double.toString(nvec[1]), Double.toString(nvec[2])
    };
  }

  public static double[] NVectorToLatLong(double[] n) {
    return new double[] {
      Math.asin(n[2]) * (180 / Math.PI), Math.atan(n[1] / n[0]) * (180 / Math.PI)
    };
  }

  public static double[] NVectorToLatLong(String[] n) {
    return NVectorToLatLong(
        new double[] {
          Double.parseDouble(n[0]), Double.parseDouble(n[1]), Double.parseDouble(n[2])
        });
  }

  public static double NVectorDist(double[] a, double[] b) {
    return NVectorDist(a, b, EARTH_MEAN_RADIUS_KM);
  }

  public static double NVectorDist(double[] a, double[] b, double radius) {
    return radius * FastInvTrig.acos(a[0] * b[0] + a[1] * b[1] + a[2] * b[2]);
  }


}

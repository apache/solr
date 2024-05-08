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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Locale;

public class NVectorDistTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig-nvector.xml", "schema-nvector.xml");
  }

  @Override
  public void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);//for parsing lat/log correctly in this test
    super.setUp();
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testNVector() throws Exception {
    assertU(adoc("id", "0", "nvector", "52.02471051274793, -0.49007556238612354"));
    assertU(commit());
    assertJQ(
        req("defType", "lucene", "q", "*:*", "fl", "id,nvector*", "sort", "id asc"),
        "/response/docs/[0]== {"
            + "'id':'0',"
            + "'nvector_0_d1':0.6152990562577377,"
            + "'nvector_1_d1':-0.005263047078845837,"
            + "'nvector_2_d1':0.7882762026750415,"
            + "'nvector':'52.02471051274793, -0.49007556238612354'}");

    assertJQ(
        req(
            "defType", "lucene",
            "q", "*:*",
            "nvd", "nvdist(52.01966071979866, -0.4983083573742952,nvector)",
            "fl", "dist:$nvd",
            "sort", "$nvd asc"),
        "/response/docs/[0]/dist==0.7953814512052634");
  }

  @Test
  public void testNVectorRadiusFilter() throws Exception {
    assertU(adoc("id", "0", "nvector", "52.02471051274793, -0.49007556238612354"));
    assertU(adoc("id", "1", "nvector", "51.927619, -0.186636"));
    assertU(adoc("id", "2", "nvector", "51.480043,  -0.196508"));
    assertU(commit());

    assertJQ(
            req(
                    "defType", "lucene",
                    "lat", "52.01966071979866",
                    "lon", "-0.4983083573742952",
                    "dist", "nvdist($lat,$lon,nvector)",
                    "q", "*:*",
                    "fl","id",
                    "sort", "$dist asc"),
            "/response/numFound==3",
            "/response/docs/[0]/id=='0'",
            "/response/docs/[1]/id=='1'"
    );

    assertJQ(
            req(
                    "defType", "lucene",
                    "lat", "52.01966071979866",
                    "lon", "-0.4983083573742952",
                    "dist", "nvdist($lat,$lon,nvector)",
                    "q", "*:*",
                    "fl","id",
                    "sort", "$dist desc"),
            "/response/numFound==3",
            "/response/docs/[0]/id=='2'",
            "/response/docs/[1]/id=='1'"
    );

    assertJQ(
            req(
                    "defType", "lucene",
                    "lat", "52.01966071979866",
                    "lon", "-0.4983083573742952",
                    "dist", "nvdist($lat,$lon,nvector)",
                    "q", "*:*",
                    "fl","id,dist:$dist",
                    "sort", "$dist asc"),
            "/response/numFound==3",
            "/response/docs/[0]/id=='0'",
            "/response/docs/[0]/dist==0.7953814512052634",
            "/response/docs/[1]/id=='1'",
            "/response/docs/[1]/dist==23.675588801593264",
            "/response/docs/[2]/id=='2'",
            "/response/docs/[2]/dist==63.49776326818523"
    );

    assertJQ(
            req(
                    "defType", "lucene",
                    "lat", "52.01966071979866",
                    "lon", "-0.4983083573742952",
                    "dist", "nvdist($lat,$lon,nvector)",
                    "q", "*:*",
                    "fl","id,dist:$dist",
                    "sort", "$dist desc"),
            "/response/numFound==3",
            "/response/docs/[0]/id=='2'",
            "/response/docs/[0]/dist==63.49776326818523",
            "/response/docs/[1]/id=='1'",
            "/response/docs/[1]/dist==23.675588801593264",
            "/response/docs/[2]/id=='0'",
            "/response/docs/[2]/dist==0.7953814512052634"


            );

    assertJQ(
        req(
            "defType", "lucene",
            "q", "{!frange u=30}nvdist(52.01966071979866, -0.4983083573742952,nvector)",
            "fl", "id,dist:nvdist(52.01966071979866, -0.4983083573742952,nvector)",
            "sort", "nvdist(52.01966071979866, -0.4983083573742952,nvector) asc"),
        "/response/numFound==2",
        "/response/docs/[0]/id=='0'",
        "/response/docs/[0]/dist==0.7953814512052634",
        "/response/docs/[1]/id=='1'",
        "/response/docs/[1]/dist==23.675588801562068");

    assertJQ(
        req(
            "defType", "lucene",
            "dist", "nvdist(52.01966071979866, -0.4983083573742952,nvector)",
            "q", "{!frange u=30}$dist",
            "fl", "id,dist:$dist",
            "sort", "$dist asc"),
        "/response/numFound==2",
        "/response/docs/[0]/id=='0'",
        "/response/docs/[0]/dist==0.7953814512052634",
        "/response/docs/[1]/id=='1'",
        "/response/docs/[1]/dist==23.675588801562068");

    assertJQ(
        req(
            "defType", "lucene",
            "lat", "52.01966071979866",
            "lon", "-0.4983083573742952",
            "dist", "nvdist($lat,$lon,nvector)",
            "q", "*:*",
            "fq", "{!frange u=30}$dist",
            "fl", "id,dist:$dist",
            "sort", "$dist asc"),
        "/response/numFound==2",
        "/response/docs/[0]/id=='0'",
        "/response/docs/[0]/dist==0.7953814512052634",
        "/response/docs/[1]/id=='1'",
        "/response/docs/[1]/dist==23.675588801562068");

  }
}

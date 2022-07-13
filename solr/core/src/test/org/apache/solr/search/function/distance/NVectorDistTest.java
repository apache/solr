
//Copyright (c) 2021, Dan Rosher
//    All rights reserved.
//
//    This source code is licensed under the BSD-style license found in the
//    LICENSE file in the root directory of this source tree.

package org.apache.solr.search.function.distance;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class NVectorDistTest extends SolrTestCaseJ4 {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
        initCore("solrconfig-nvector.xml", "schema-nvector.xml");
    }

    @Test
    public void testNVector() throws Exception {
        assertU(adoc("id", "0", "nvector", "52.02471051274793, -0.49007556238612354"));
        assertU(commit());
        assertJQ(req("defType", "lucene", "q", "*:*", "fl", "id,nvector*","sort","id asc"),
            "/response/docs/[0]== {" +
                "'id':'0'," +
                "'nvector_0_d1':0.6152990562577377," +
                "'nvector_1_d1':-0.005263047078845837," +
                "'nvector_2_d1':0.7882762026750415," +
                "'nvector':'52.02471051274793, -0.49007556238612354'}");

        assertJQ(req(
            "defType", "lucene",
            "q", "*:*",
            "nvd","nvdist(52.01966071979866, -0.4983083573742952,nvector)",
            "fl", "dist:$nvd",
            "sort" ,"$nvd asc"),
            "/response/docs/[0]/dist==0.7706622667641961");
    }

    @Test
    public void testNVectorRadiusFilter() throws Exception {
        assertU(adoc("id", "0", "nvector", "52.02471051274793, -0.49007556238612354"));
        assertU(adoc("id", "1", "nvector", "51.927619, -0.186636"));
        assertU(adoc("id", "2", "nvector", "51.480043,  -0.196508"));
        assertU(commit());
        assertJQ(req(
            "defType", "lucene",
            "q", "{!frange u=30}nvdist(52.01966071979866, -0.4983083573742952,nvector)",
            "fl", "id,dist:nvdist(52.01966071979866, -0.4983083573742952,nvector)",
            "sort","nvdist(52.01966071979866, -0.4983083573742952,nvector) asc"),
            "/response/numFound==2",
            "/response/docs/[0]/id=='0'",
            "/response/docs/[0]/dist==0.7706622667641961",
            "/response/docs/[1]/id=='1'",
            "/response/docs/[1]/dist==22.939789336475414"
            );

        assertJQ(req(
            "defType", "lucene",
            "dist","nvdist(52.01966071979866, -0.4983083573742952,nvector)",
            "q", "{!frange u=30}$dist",
            "fl", "id,dist:$dist",
            "sort","$dist asc"),
            "/response/numFound==2",
            "/response/docs/[0]/id=='0'",
            "/response/docs/[0]/dist==0.7706622667641961",
            "/response/docs/[1]/id=='1'",
            "/response/docs/[1]/dist==22.939789336475414"
        );

        assertJQ(req(
            "defType", "lucene",
            "lat","52.01966071979866",
            "lon","-0.4983083573742952",
            "dist","nvdist($lat,$lon,nvector)",
            "q","*:*",
            "fq", "{!frange u=30}$dist",
            "fl", "id,dist:$dist",
            "sort","$dist asc"
            ),
            "/response/numFound==2",
            "/response/docs/[0]/id=='0'",
            "/response/docs/[0]/dist==0.7706622667641961",
            "/response/docs/[1]/id=='1'",
            "/response/docs/[1]/dist==22.939789336475414"
        );
    }
}

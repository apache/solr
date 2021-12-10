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
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 *
 **/
public class DistanceFunctionTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-spatial.xml");
  }

  @Test
  public void testHaversine() throws Exception {
    clearIndex();
    assertU(adoc("id", "1", "x_td", "0", "y_td", "0"));
    assertU(adoc("id", "2", "x_td", "0", "y_td", String.valueOf(Math.PI / 2)));
    assertU(adoc("id", "3", "x_td", String.valueOf(Math.PI / 2), "y_td", String.valueOf(Math.PI / 2)));
    assertU(adoc("id", "4", "x_td", String.valueOf(Math.PI / 4), "y_td", String.valueOf(Math.PI / 4)));
    assertU(adoc("id", "5", "x_td", "45.0", "y_td", "45.0"));
    assertU(adoc("id", "6", "point", "32.5, -79.0"));
    assertU(adoc("id", "7", "point", "32.6, -78.0"));
    assertU(commit());
    //Get the haversine distance between the point 0,0 and the docs above assuming a radius of 1
    assertQ(req("fl", "*,score", "q", "{!func}hsin(1, false, x_td, y_td, 0, 0)", "fq", "id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl", "*,score", "q", "{!func}hsin(1, false, x_td, y_td, 0, 0)", "fq", "id:2"), "//float[@name='score']='" + (float) (Math.PI / 2) + "'");
    assertQ(req("fl", "*,score", "q", "{!func}hsin(1, false, x_td, y_td, 0, 0)", "fq", "id:3"), "//float[@name='score']='" + (float) (Math.PI / 2) + "'");
    assertQ(req("fl", "*,score", "q", "{!func}hsin(1, false, x_td, y_td, 0, 0)", "fq", "id:4"), "//float[@name='score']='1.0471976'");
    assertQ(req("fl", "*,score", "q", "{!func}hsin(1, true, x_td, y_td, 0, 0)", "fq", "id:5"), "//float[@name='score']='1.0471976'");
    //SOLR-2114
    assertQ(req("fl", "*,score", "q", "{!func}hsin(6371.009, true, point, vector(0, 0))", "fq", "id:6"), "//float[@name='score']='8977.814'");
  }


  @Test
  public void testLatLon() throws Exception {
    String sfield = "llp_km"; // primary spatial field
    double delta = 1e-4;

    // "lat,lon" order
    assertU(adoc("id", "100", sfield, "1,2", "srpt_geohash_km", "1,2")); // secondary spatial is equal
    assertU(commit());
   
    assertJQ(req("defType","func", 
                 "q","geodist(1,2,3,4)",
                 "fq","id:100",
                 "fl","id,score")
             , delta
             , "/response/docs/[0]/score==314.40338"
             );

    // throw in some decimal points
    assertJQ(req("defType","func", 
                 "q","geodist(1.0,2,3,4.0)",
                 "fq","id:100",
                 "fl","id,score")
             , delta
             , "/response/docs/[0]/score==314.40338"
             );

    // default to reading pt
    assertJQ(req("defType","func", 
                 "q","geodist(1,2)",
                 "pt","3,4", 
                 "fq","id:100",
                 "fl","id,score")
             , delta
             , "/response/docs/[0]/score==314.40338"
             );

    // default to reading pt first
    assertJQ(req("defType","func", 
                 "q","geodist(1,2)",
                 "pt","3,4", 
                 "sfield", sfield,
                 "fq","id:100",
                 "fl","id,score")
             , delta
             , "/response/docs/[0]/score==314.40338"
             );

    // if pt missing, use sfield
    assertJQ(req("defType","func",
                 "q","geodist(3,4)",
                 "sfield", sfield,
                 "fq","id:100",
                 "fl","id,score")
             , delta
             ,"/response/docs/[0]/score==314.40338"
             );

    // if pt missing, use sfield (RPT)
    assertJQ(req("defType","func",
        "q","geodist(3,4)",
        "sfield","srpt_geohash_km",
        "fq","id:100",
        "fl","id,score")
        , delta
        ,"/response/docs/[0]/score==314.40338"
    );
    
    // read both pt and sfield
    assertJQ(req("defType","func",
                 "q","geodist()","pt","3,4",
                 "sfield", sfield,
                 "fq","id:100",
                 "fl","id,score")
             , delta
             ,"/response/docs/[0]/score==314.40338"
             );

    // read both pt and sfield (RPT)
    assertJQ(req("defType","func",
        "q","geodist()","pt","3,4",
        "sfield","srpt_geohash_km",
        "fq","id:100",
        "fl","id,score")
        , delta
        ,"/response/docs/[0]/score==314.40338"
    );

    // param substitution
    assertJQ(req("defType","func", 
                 "q","geodist($a)",
                 "a","3,4",
                 "sfield", sfield,
                 "fq","id:100",
                 "fl","id,score")
             , delta
             ,"/response/docs/[0]/score==314.40338"
             );

  }

  
  @Test
  public void testVector() throws Exception {
    clearIndex();
    assertU(adoc("id", "1", "x_td", "0", "y_td", "0", "z_td", "0", "w_td", "0"));
    assertU(adoc("id", "2", "x_td", "0", "y_td", "1", "z_td", "0", "w_td", "0"));
    assertU(adoc("id", "3", "x_td", "1", "y_td", "1", "z_td", "1", "w_td", "1"));
    assertU(adoc("id", "4", "x_td", "1", "y_td", "0", "z_td", "0", "w_td", "0"));
    assertU(adoc("id", "5", "x_td", "2.3", "y_td", "5.5", "z_td", "7.9", "w_td", "-2.4"));
    assertU(adoc("id", "6", "point", "1.0,0.0"));
    assertU(adoc("id", "7", "point", "5.5,10.9"));
    assertU(commit());
    //two dimensions, notice how we only pass in 4 value sources
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, 0, 0)", "fq", "id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, 0, 0)", "fq", "id:2"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, 0, 0)", "fq", "id:3"), "//float[@name='score']='" + 2.0f + "'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, 0, 0)", "fq", "id:4"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, 0, 0)", "fq", "id:5"), "//float[@name='score']='" + (float) (2.3 * 2.3 + 5.5 * 5.5) + "'");

    //three dimensions, notice how we pass in 6 value sources
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, 0, 0, 0)", "fq", "id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, 0, 0, 0)", "fq", "id:2"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, 0, 0, 0)", "fq", "id:3"), "//float[@name='score']='" + 3.0f + "'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, 0, 0, 0)", "fq", "id:4"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, 0, 0, 0)", "fq", "id:5"), "//float[@name='score']='" + (float) (2.3 * 2.3 + 5.5 * 5.5 + 7.9 * 7.9) + "'");

    //four dimensions, notice how we pass in 8 value sources
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, w_td, 0, 0, 0, 0)", "fq", "id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, w_td, 0, 0, 0, 0)", "fq", "id:2"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, w_td, 0, 0, 0, 0)", "fq", "id:3"), "//float[@name='score']='" + 4.0f + "'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, w_td, 0, 0, 0, 0)", "fq", "id:4"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, w_td, 0, 0, 0, 0)", "fq", "id:5"), "//float[@name='score']='" + (float) (2.3 * 2.3 + 5.5 * 5.5 + 7.9 * 7.9 + 2.4 * 2.4) + "'");
    //Pass in imbalanced list, throw exception
    try {
      ignoreException("Illegal number of sources");
      assertQ(req("fl", "*,score", "q", "{!func}sqedist(x_td, y_td, z_td, w_td, 0, 0, 0)", "fq", "id:1"), "//float[@name='score']='0.0'");
      assertTrue("should throw an exception", false);
    } catch (Exception e) {
      Throwable cause = e.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof SolrException);
    }
    resetExceptionIgnores();

    //do one test of Euclidean
    //two dimensions, notice how we only pass in 4 value sources
    assertQ(req("fl", "*,score", "q", "{!func}dist(2, x_td, y_td, 0, 0)", "fq", "id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl", "*,score", "q", "{!func}dist(2, x_td, y_td, 0, 0)", "fq", "id:2"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}dist(2, x_td, y_td, 0, 0)", "fq", "id:3"), "//float[@name='score']='" + (float) Math.sqrt(2.0) + "'");
    assertQ(req("fl", "*,score", "q", "{!func}dist(2, x_td, y_td, 0, 0)", "fq", "id:4"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}dist(2, x_td, y_td, 0, 0)", "fq", "id:5"), "//float[@name='score']='" + (float) Math.sqrt((2.3 * 2.3 + 5.5 * 5.5)) + "'");

    //do one test of Manhattan
    //two dimensions, notice how we only pass in 4 value sources
    assertQ(req("fl", "*,score", "q", "{!func}dist(1, x_td, y_td, 0, 0)", "fq", "id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl", "*,score", "q", "{!func}dist(1, x_td, y_td, 0, 0)", "fq", "id:2"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}dist(1, x_td, y_td, 0, 0)", "fq", "id:3"), "//float[@name='score']='" + (float) 2.0 + "'");
    assertQ(req("fl", "*,score", "q", "{!func}dist(1, x_td, y_td, 0, 0)", "fq", "id:4"), "//float[@name='score']='1.0'");
    assertQ(req("fl", "*,score", "q", "{!func}dist(1, x_td, y_td, 0, 0)", "fq", "id:5"), "//float[@name='score']='" + (float) (2.3 + 5.5) + "'");


    //Do point tests:
    assertQ(req("fl", "*,score", "q", "{!func}dist(1, vector(x_td, y_td), vector(0, 0))", "fq", "id:5"),
            "//float[@name='score']='" + (float) (2.3 + 5.5) + "'");

    assertQ(req("fl", "*,score", "q", "{!func}dist(1, point, vector(0, 0))", "fq", "id:6"),
            "//float[@name='score']='" + 1.0f + "'");

  }

}

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

package org.apache.solr.search.function.decayfunction;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGaussDecayParser extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-decayfunc.xml", "schema-decayfunc.xml");
  }

  @Test
  public void testIntNumericGaussDecayParser() throws Exception {
    clearIndex();
    int[] vals = new int[] {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80};
    for (int i = 0; i < vals.length; i++) {
      assertU(adoc("id", Integer.toString(i), "test_i1", Integer.toString(vals[i])));
    }
    assertU(commit());

    assertJQ(
        req(
            "df", "id",
            "b", "gauss(test_i1,5,40,5,0.5)", // field,scale,origin,offset,decay
            "q", "{!boost b=$b v=$qq}",
            "qq", "{!edismax }*:*",
            "sort", "score desc",
            "rows", Integer.toString(vals.length),
            "fl", "*,score"),
        "/response/docs/[0]/test_i1==35",
        "/response/docs/[0]/score==1.0",
        "/response/docs/[1]/test_i1==40",
        "/response/docs/[1]/score==1.0",
        "/response/docs/[2]/test_i1==45",
        "/response/docs/[2]/score==1.0",
        "/response/docs/[3]/test_i1==30",
        "/response/docs/[3]/score==0.5",
        "/response/docs/[4]/test_i1==50",
        "/response/docs/[4]/score==0.5",
        "/response/docs/[5]/test_i1==25",
        "/response/docs/[5]/score==0.0625");

    clearIndex();
    vals = new int[] {0, 10, 20, 50, 80, 100};
    for (int i = 0; i < vals.length; i++) {
      assertU(adoc("id", Integer.toString(i), "test_i1", Integer.toString(vals[i])));
    }
    assertU(commit());

    assertJQ(
        req(
            "df", "id",
            "b", "gauss(test_i1,20,50,20,0.5)", // field,scale,origin,offset,decay
            "q", "{!boost b=$b v=$qq}",
            "qq", "{!edismax }*:*",
            "sort", "score desc",
            "rows", Integer.toString(vals.length),
            "fl", "*,score"),
        "/response/docs/[0]/test_i1==50",
        "/response/docs/[0]/score==1.0",
        "/response/docs/[1]/test_i1==20",
        "/response/docs/[1]/score==0.8408964",
        "/response/docs/[2]/test_i1==80",
        "/response/docs/[2]/score==0.8408964",
        "/response/docs/[3]/test_i1==10",
        "/response/docs/[3]/score==0.5",
        "/response/docs/[4]/test_i1==0",
        "/response/docs/[4]/score==0.2102241",
        "/response/docs/[5]/test_i1==100",
        "/response/docs/[5]/score==0.2102241");
  }

  @Test
  public void testDatePointFieldGaussDecayParser() throws Exception {

    clearIndex();
    int[] vals =
        new int[] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29};
    for (int i = 0; i < vals.length; i++) {
      assertU(adoc("id", Integer.toString(i), "test_date_p", "2021-07-" + vals[i] + "T00:00:00Z"));
    }
    assertU(commit());
    assertJQ(
        req(
            "df", "id",
            "b",
                "gauss(test_date_p,\"+2DAY+6HOUR\",\"2021-07-20T00:00:00Z\",\"+3DAY\",0.5)", // field,scale,origin,offset,decay
            "q", "{!boost b=$b v=$qq}",
            "qq", "{!edismax }*:*",
            "sort", "score desc",
            "rows", Integer.toString(vals.length),
            "fl", "*,score"),
        "/response/docs/[0]/test_date_p=='2021-07-17T00:00:00Z'",
        "/response/docs/[0]/score==1.0",
        "/response/docs/[1]/test_date_p=='2021-07-18T00:00:00Z'",
        "/response/docs/[1]/score==1.0",
        "/response/docs/[2]/test_date_p=='2021-07-19T00:00:00Z'",
        "/response/docs/[2]/score==1.0",
        "/response/docs/[3]/test_date_p=='2021-07-20T00:00:00Z'",
        "/response/docs/[3]/score==1.0",
        "/response/docs/[4]/test_date_p=='2021-07-21T00:00:00Z'",
        "/response/docs/[4]/score==1.0",
        "/response/docs/[5]/test_date_p=='2021-07-22T00:00:00Z'",
        "/response/docs/[5]/score==1.0",
        "/response/docs/[6]/test_date_p=='2021-07-23T00:00:00Z'",
        "/response/docs/[6]/score==1.0",
        "/response/docs/[7]/test_date_p=='2021-07-16T00:00:00Z'",
        "/response/docs/[7]/score==0.87204176",
        "/response/docs/[8]/test_date_p=='2021-07-24T00:00:00Z'",
        "/response/docs/[8]/score==0.87204176",
        "/response/docs/[9]/test_date_p=='2021-07-15T00:00:00Z'",
        "/response/docs/[9]/score==0.5782946}",
        "/response/docs/[10]/test_date_p=='2021-07-25T00:00:00Z'",
        "/response/docs/[10]/score==0.5782946}",
        "/response/docs/[11]/test_date_p=='2021-07-14T00:00:00Z'",
        "/response/docs/[11]/score==0.29163226",
        "/response/docs/[12]/test_date_p=='2021-07-26T00:00:00Z'",
        "/response/docs/[12]/score==0.29163226",
        "/response/docs/[13]/test_date_p=='2021-07-13T00:00:00Z'",
        "/response/docs/[13]/score==0.111839846",
        "/response/docs/[14]/test_date_p=='2021-07-27T00:00:00Z'",
        "/response/docs/[14]/score==0.111839846",
        "/response/docs/[15]/test_date_p=='2021-07-12T00:00:00Z'",
        "/response/docs/[15]/score==0.032616105");
  }

  @Test
  public void testLatLonPointFieldGaussDecayParser() throws Exception {
    clearIndex();
    assertU(adoc("id", "0", "test_loc1_dv", "52.02471051274793, -0.49007556238612354"));
    assertU(adoc("id", "1", "test_loc1_dv", "51.927619, -0.186636"));
    assertU(adoc("id", "2", "test_loc1_dv", "51.480043,  -0.196508"));

    assertU(commit());
    assertJQ(
        req(
            "df", "id",
            "b",
                "gauss(test_loc1_dv,\"23.420770393818795km\",52.02471051274793, -0.49007556238612354,\"0km\",0.5)", // field,scale,origin_lat,origin_lon,offset,decay
            "q", "{!boost b=$b v=$qq}",
            "qq", "{!lucene }*:*",
            "sort", "score desc",
            "rows", "3",
            "fl", "*,score"),
        "/response/docs/[0]/test_loc1_dv=='52.02471051274793, -0.49007556238612354'",
        "/response/docs/[0]/score==1.0",
        "/response/docs/[1]/test_loc1_dv=='51.927619, -0.186636'",
        "/response/docs/[1]/score==0.5",
        "/response/docs/[2]/test_loc1_dv=='51.480043,  -0.196508'",
        "/response/docs/[2]/score==0.0057930867");
  }
}

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
package org.apache.solr.search.mlt;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class SimpleMLTContentQParserTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void moreLikeThisBeforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void doTest() {
    SimpleMLTQParserTest.setupDocsForMLT();

    // for score tiebreaker, use doc ID order
    final SolrParams sortParams = params("sort", "score desc, id asc");

    final String seventeenth =
        "The quote red fox jumped moon over the lazy "
            + "brown dogs moon. Of course moon. Foxes and moon come back to the foxes and moon";
    assertQ(
        req(
            sortParams,
            CommonParams.Q,
            "{!mlt_content qf=lowerfilt mintf=0 mindf=0}" + seventeenth),
        "//result/doc[1]/str[@name='id'][.='17']");
    assertQ(
        req(
            sortParams,
            CommonParams.Q,
            "{!mlt_content qf=lowerfilt}" + seventeenth,
            "fq",
            "-id:17"),
        "//result/doc[1]/str[@name='id'][.='13']",
        "//result/doc[2]/str[@name='id'][.='14']",
        "//result/doc[3]/str[@name='id'][.='15']",
        "//result/doc[4]/str[@name='id'][.='16']",
        "//result/doc[5]/str[@name='id'][.='18']",
        "//result/doc[6]/str[@name='id'][.='19']",
        "//result/doc[7]/str[@name='id'][.='20']",
        "//result/doc[8]/str[@name='id'][.='21']",
        "//result/doc[9]/str[@name='id'][.='22']",
        "//result/doc[10]/str[@name='id'][.='23']");

    assertQ(
        req(
            sortParams,
            CommonParams.Q,
            "{!mlt_content qf=lowerfilt boost=true}" + seventeenth,
            "fq",
            "-id:17"),
        "//result/doc[1]/str[@name='id'][.='13']",
        "//result/doc[2]/str[@name='id'][.='14']",
        "//result/doc[3]/str[@name='id'][.='15']",
        "//result/doc[4]/str[@name='id'][.='16']",
        "//result/doc[5]/str[@name='id'][.='18']",
        "//result/doc[6]/str[@name='id'][.='19']",
        "//result/doc[7]/str[@name='id'][.='20']",
        "//result/doc[8]/str[@name='id'][.='21']",
        "//result/doc[9]/str[@name='id'][.='22']",
        "//result/doc[10]/str[@name='id'][.='23']");
    final String thirteenth = "The quote red fox jumped over the lazy brown dogs.";
    final String thirteenth1 = "red green yellow";
    assertQ(
        req(
            sortParams,
            CommonParams.Q,
            " " /*space matterz*/
                + "{!mlt_content qf=lowerfilt boost=false mintf=0 mindf=0 v=$lowerfilt30} "
                + "{!mlt_content qf=lowerfilt1^1000 boost=false mintf=0 mindf=0 v=$lowerfilt130}",
            "lowerfilt30",
            thirteenth,
            "lowerfilt130",
            thirteenth1,
            "fl",
            "lowerfilt,lowerfilt1,id,score",
            "explainOther",
            "id:31",
            "indent",
            "on",
            "debugQuery",
            "on"),
        "//result/doc[1]/str[@name='id'][.='30']",
        "//result/doc[2]/str[@name='id'][.='31']");
    assertQ(
        req(
            sortParams,
            CommonParams.Q,
            " " /*space matterz*/
                + "{!mlt_content qf=lowerfilt,lowerfilt1^1000 boost=false mintf=0 mindf=0 v=$lowerfilt30} ",
            "lowerfilt30",
            thirteenth + " " + thirteenth1),
        "//result/doc[1]/str[@name='id'][.='30']",
        "//result/doc[2]/str[@name='id'][.='31']");
    assertQ(
        req(
            sortParams,
            CommonParams.Q,
            "{!mlt_content qf=lowerfilt,lowerfilt1^1000 boost=false mintf=0 mindf=0}"
                + thirteenth
                + " "
                + thirteenth1,
            "fq",
            "-id:30"),
        "//result/doc[1]/str[@name='id'][.='31']",
        "//result/doc[2]/str[@name='id'][.='13']",
        "//result/doc[3]/str[@name='id'][.='14']",
        "//result/doc[4]/str[@name='id'][.='18']",
        "//result/doc[5]/str[@name='id'][.='20']",
        "//result/doc[6]/str[@name='id'][.='22']",
        "//result/doc[7]/str[@name='id'][.='23']",
        "//result/doc[8]/str[@name='id'][.='32']",
        "//result/doc[9]/str[@name='id'][.='15']",
        "//result/doc[10]/str[@name='id'][.='16']");

    assertQ(
        req(
            sortParams,
            CommonParams.Q,
            "{!mlt_content qf=lowerfilt,lowerfilt1^1000 boost=true mintf=0 mindf=0}"
                + thirteenth
                + " "
                + thirteenth1,
            "fq",
            "-id:30"),
        "//result/doc[1]/str[@name='id'][.='29']",
        "//result/doc[2]/str[@name='id'][.='31']",
        "//result/doc[3]/str[@name='id'][.='32']",
        "//result/doc[4]/str[@name='id'][.='13']",
        "//result/doc[5]/str[@name='id'][.='14']",
        "//result/doc[6]/str[@name='id'][.='18']",
        "//result/doc[7]/str[@name='id'][.='20']",
        "//result/doc[8]/str[@name='id'][.='22']",
        "//result/doc[9]/str[@name='id'][.='23']",
        "//result/doc[10]/str[@name='id'][.='15']");

    String s26th = "bmw usa 328i";
    assertQ(
        req(
            sortParams,
            CommonParams.Q,
            "{!mlt_content qf=lowerfilt mindf=0 mintf=1}" + s26th,
            "fq",
            "-id:26"),
        "//result/doc[1]/str[@name='id'][.='29']",
        "//result/doc[2]/str[@name='id'][.='27']",
        "//result/doc[3]/str[@name='id'][.='28']");

    assertQ(
        req(CommonParams.Q, "{!mlt_content qf=lowerfilt mindf=10 mintf=1}" + s26th, "fq", "-id:26"),
        "//result[@numFound='0']");

    assertQ(
        req(
            CommonParams.Q,
            "{!mlt_content qf=lowerfilt minwl=3 mintf=1 mindf=1}" + s26th,
            "fq",
            "-id:26"),
        "//result[@numFound='3']");

    assertQ(
        req(
            CommonParams.Q,
            "{!mlt_content qf=lowerfilt minwl=4 mintf=1 mindf=1}" + s26th,
            "fq",
            "-id:26",
            CommonParams.DEBUG,
            "true"),
        "//result[@numFound='0']");
  }
}

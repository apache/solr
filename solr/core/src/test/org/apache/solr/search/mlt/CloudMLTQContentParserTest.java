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

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CloudMLTQContentParserTest extends SolrCloudTestCase {

  private final String seventeenth =
      "The quote red fox jumped moon over the lazy brown dogs moon."
          + " Of course moon. Foxes and moon come back to the foxes and moon";

  @Before
  public void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-dynamic")).configure();

    CloudMLTQParserTest.indexDocs();
  }

  @After
  public void cleanCluster() throws Exception {
    if (null != cluster) {
      cluster.shutdown();
    }
  }

  public static final String COLLECTION = "mlt-collection";

  @Test
  public void testMLTQParser() throws Exception {

    QueryResponse queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery(
                        "q", "{!mlt_content qf=lowerfilt_u mindf=0}" + seventeenth, "fq", "-id:17")
                    .setShowDebugInfo(true));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[] {7, 9, 13, 14, 15, 16, 20, 22, 24, 32};
    int[] actualIds = new int[10];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }

    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);
  }

  @Test
  public void testBoost() throws Exception {

    QueryResponse queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery(
                    "q",
                    "{!mlt_content qf=lowerfilt_u boost=true mindf=0}" + seventeenth,
                    "fq",
                    "-id:17"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[] {7, 9, 13, 14, 15, 16, 20, 22, 24, 32};
    int[] actualIds = new int[solrDocuments.size()];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }

    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);

    String thirtineenth = "The quote red fox jumped over the lazy brown dogs." + "red green yellow";
    queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery(
                    "q",
                    "{!mlt_content qf=lowerfilt_u^10,lowerfilt1_u^1000 boost=false mintf=0 mindf=0}"
                        + thirtineenth,
                    "fq",
                    "-id:30"));
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[] {31, 18, 23, 13, 14, 20, 22, 32, 19, 21};
    actualIds = new int[solrDocuments.size()];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }

    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);

    queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery(
                    "q",
                    "{!mlt_content qf=lowerfilt_u^10,lowerfilt1_u^1000 boost=true mintf=0 mindf=0}"
                        + thirtineenth,
                    "fq",
                    "-id:30"));
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[] {29, 31, 32, 18, 23, 13, 14, 20, 22, 19};
    actualIds = new int[solrDocuments.size()];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }

    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testMinDF() throws Exception {

    QueryResponse queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery(
                        "q",
                        "{!mlt_content qf=lowerfilt_u mindf=0 mintf=1}" + "bmw usa",
                        "fq",
                        "-id:3")
                    .setShowDebugInfo(true));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[] {29, 27, 26, 28};
    int[] actualIds = new int[solrDocuments.size()];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }

    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);

    String[] expectedQueryStrings =
        new String[] {"lowerfilt_u:bmw lowerfilt_u:usa", "lowerfilt_u:usa lowerfilt_u:bmw"};

    String[] actualParsedQueries;
    if (queryResponse.getDebugMap().get("parsedquery") instanceof String) {
      String parsedQueryString = (String) queryResponse.getDebugMap().get("parsedquery");
      assertTrue(
          parsedQueryString.equals(expectedQueryStrings[0])
              || parsedQueryString.equals(expectedQueryStrings[1]));
    } else {
      actualParsedQueries =
          ((ArrayList<String>) queryResponse.getDebugMap().get("parsedquery"))
              .toArray(new String[0]);
      Arrays.sort(actualParsedQueries);
      assertArrayEquals(expectedQueryStrings, actualParsedQueries);
    }
  }

  @Test
  public void testMultipleFields() throws Exception {

    QueryResponse queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery(
                    "q",
                    "{!mlt_content qf=lowerfilt_u,lowerfilt1_u mindf=0 mintf=1}" + "bmw usa 328i",
                    "fq",
                    "-id:26"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[] {3, 29, 27, 28};
    int[] actualIds = new int[solrDocuments.size()];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }

    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);
  }

  @Test
  public void testHighDFValue() throws Exception {

    // Test out a high value of df and make sure nothing matches.
    QueryResponse queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery("q", "{!mlt_content qf=lowerfilt_u mindf=20 mintf=1}" + "bmw usa"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    assertEquals(
        "Expected to match 0 documents with a mindf of 20 but found more", solrDocuments.size(), 0);
  }

  @Test
  public void testHighWLValue() throws Exception {

    // Test out a high value of wl and make sure nothing matches.
    QueryResponse queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery("q", "{!mlt_content qf=lowerfilt_u minwl=4 mintf=1}" + "bmw usa"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    assertEquals(
        "Expected to match 0 documents with a minwl of 4 but found more", solrDocuments.size(), 0);
  }

  @Test
  public void testLowMinWLValue() throws Exception {

    // Test out a low enough value of minwl and make sure we get the expected matches.
    QueryResponse queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery(
                    "q",
                    "{!mlt_content qf=lowerfilt_u minwl=3 mintf=1 mindf=0}" + "bmw usa",
                    "fq",
                    "-id:3"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    assertEquals(
        "Expected to match 4 documents with a minwl of 3 but found more", 4, solrDocuments.size());
  }

  // there's a problem with this feature {!mlt}<id> picks only fields from the doc
  // but here I have to specify field explicitly otherwise it picks ou all fields from schema and
  // fails on analysin UUID field
  @Test
  public void testUnstoredAndUnanalyzedFieldsAreIgnored() throws Exception {
    QueryResponse queryResponse =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                new SolrQuery(
                    "q",
                    "{!mlt_content qf=lowerfilt_u}"
                        + "The quote red fox jumped over the lazy brown dogs.",
                    "fq",
                    "-id:20"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] actualIds = new int[solrDocuments.size()];
    int[] expectedIds = new int[] {13, 14, 15, 16, 22, 24, 32, 18, 19, 21};
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }

    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);
  }
}
